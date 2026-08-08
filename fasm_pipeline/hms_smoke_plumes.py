"""HMS smoke plumes ingest -> pwfsl_map.hms_smoke_plume (+ S3 status file)."""

import json
import logging
from datetime import datetime, timedelta, timezone

import geopandas as gpd
from shapely.geometry import shape

from fasm_pipeline import config
from fasm_pipeline.db import get_ts_db_conn, get_ts_engine
from fasm_pipeline.s3 import (
    airfire_exports_bucket,
    fasm_layers_bucket,
    init_epa_s3,
    init_s3,
)
from fasm_pipeline.time_util import parse_hms_smoke_datetime

logger = logging.getLogger(__name__)


def extract():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.HMS_SMOKE_S3_KEY)
    data = json.loads(results["Body"].read())
    features = data["features"]
    logger.info(f"Retrieved {len(features)} HMS features from S3")
    return features


def transform(data):
    """
    HMS Data is in CRS84 and not EPSG:4326, which is essentially EPSG:4326
    but it's lon/lat instead of lat/lon. Setting the CRS seems to work.
    """
    parsed_data = [
        [
            item["properties"]["Satellite"],
            item["properties"]["Density"],
            parse_hms_smoke_datetime(item["properties"]["Start"]),
            parse_hms_smoke_datetime(item["properties"]["End"]),
            shape(item["geometry"]),
        ]
        for item in data
    ]
    gdf = gpd.GeoDataFrame(
        data=parsed_data,
        columns=["satellite", "density", "start_utc", "end_utc", "geom"],
    )
    gdf = gdf.set_geometry("geom")
    gdf.geom = gdf.geom.set_crs(epsg=4326)

    initial_count = len(gdf)
    gdf = gdf.drop_duplicates(subset=["geom"])
    final_count = len(gdf)

    if initial_count != final_count:
        logger.info(f"Removed {initial_count - final_count} duplicate geometries")
    logger.info(f"Built GeoDataFrame with {final_count} features")
    return gdf


def truncate():
    table = config.qualified(config.HMS_SMOKE_TABLE)
    conn = get_ts_db_conn()
    with conn.cursor() as c:
        c.execute(f"TRUNCATE {table};")
    conn.commit()
    conn.close()
    logger.info(f"Truncated {table} table")


def get_existing_table_metadata():
    """Query existing pwfsl_map.hms_smoke_plume table for row count and latest end_utc."""
    table = config.qualified(config.HMS_SMOKE_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"SELECT COUNT(*), MAX(end_utc) FROM {table};")
            row = c.fetchone()
            count = row[0] if row else 0
            max_end = row[1] if row else None
            return count, max_end
    except Exception as e:
        logger.warning(f"Could not query existing table metadata: {e}")
        return 0, None
    finally:
        conn.close()


def load(gdf):
    table = config.qualified(config.HMS_SMOKE_TABLE)
    engine = get_ts_engine()
    gdf.to_postgis(config.HMS_SMOKE_TABLE, engine, schema=config.DEST_SCHEMA, if_exists="append")
    logger.info(f"Loaded {len(gdf)} features to {table}")
    return f"💨 Loaded {len(gdf)} HMS smoke plume features successfully 💨"


def write_status_to_s3(last_scan_dt, is_fallback=False, display_date=None, features=0):
    last_checked = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if last_scan_dt:
        if isinstance(last_scan_dt, str):
            last_scan_str = last_scan_dt
            if not display_date and "T" in last_scan_dt:
                display_date = last_scan_dt.split("T")[0]
        else:
            last_scan_str = last_scan_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if not display_date:
                display_date = last_scan_dt.strftime("%Y-%m-%d")
    else:
        last_scan_str = None

    status = {
        "last_checked": last_checked,
        "last_scan": last_scan_str,
        "display_date": display_date,
        "is_fallback": is_fallback,
        "features": features,
    }

    s3 = init_epa_s3()
    try:
        s3.put_object(
            Bucket=fasm_layers_bucket(),
            Key=config.HMS_STATUS_S3_KEY,
            Body=json.dumps(status),
            ContentType="application/json",
        )
    except Exception as e:
        logger.warning(f"Could not write hms_status.json to S3: {e}")
    logger.info(
        f"Wrote hms_status.json — last_checked: {last_checked}, last_scan: {last_scan_str}, "
        f"display_date: {display_date}, is_fallback: {is_fallback}, features: {features}"
    )


def run(max_fallback_hours=6):
    """Run the full HMS smoke plumes ingest end-to-end. Returns a summary string."""
    data = extract()
    gdf = transform(data)

    if len(gdf) > 0:
        truncate()
        msg = load(gdf)
        max_end = gdf["end_utc"].max() if "end_utc" in gdf.columns and gdf["end_utc"].notna().any() else None
        write_status_to_s3(
            last_scan_dt=max_end,
            is_fallback=False,
            display_date=max_end.strftime("%Y-%m-%d") if max_end else None,
            features=len(gdf),
        )
        return msg
    else:
        existing_count, max_end_utc = get_existing_table_metadata()
        now_utc = datetime.now(timezone.utc)

        if existing_count > 0 and max_end_utc:
            if max_end_utc.tzinfo is None:
                max_end_utc = max_end_utc.replace(tzinfo=timezone.utc)

            age = now_utc - max_end_utc
            if age <= timedelta(hours=max_fallback_hours):
                logger.info(
                    f"0 new features in latest_smoke.geojson. Retaining {existing_count} existing features "
                    f"from {max_end_utc.isoformat()} (age: {age.total_seconds() / 3600:.1f}h <= {max_fallback_hours}h limit)"
                )
                write_status_to_s3(
                    last_scan_dt=max_end_utc,
                    is_fallback=True,
                    display_date=max_end_utc.strftime("%Y-%m-%d"),
                    features=existing_count,
                )
                return (
                    f"💨 0 features from NOAA; retained {existing_count} fallback HMS smoke plume "
                    f"features from {max_end_utc.strftime('%Y-%m-%d')} 💨"
                )

        logger.info(
            f"0 features in latest_smoke.geojson and no valid fallback data within {max_fallback_hours}h. Truncating table."
        )
        truncate()
        write_status_to_s3(
            last_scan_dt=None,
            is_fallback=False,
            display_date=None,
            features=0,
        )
        return "💨 0 features from NOAA; cleared HMS smoke plume table 💨"
