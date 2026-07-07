"""HMS smoke plumes ingest -> pwfsl_map.hms_smoke_plume (+ S3 status file)."""
import json
import logging
from datetime import datetime, timezone

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


def load(gdf):
    table = config.qualified(config.HMS_SMOKE_TABLE)
    engine = get_ts_engine()
    gdf.to_postgis(config.HMS_SMOKE_TABLE, engine, schema=config.DEST_SCHEMA, if_exists="append")
    logger.info(f"Loaded {len(gdf)} features to {table}")
    return f"💨 Loaded {len(gdf)} HMS smoke plume features successfully 💨"


def write_status_to_s3(gdf):
    last_checked = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if len(gdf) > 0:
        last_scan = gdf["end_utc"].max().strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        last_scan = None

    status = {
        "last_checked": last_checked,
        "last_scan": last_scan,
    }

    s3 = init_epa_s3()
    s3.put_object(
        Bucket=fasm_layers_bucket(),
        Key=config.HMS_STATUS_S3_KEY,
        Body=json.dumps(status),
        ContentType="application/json",
        ACL="public-read",
    )
    logger.info(f"Wrote hms_status.json — last_checked: {last_checked}, last_scan: {last_scan}")


def run():
    """Run the full HMS smoke plumes ingest end-to-end. Returns a summary string."""
    data = extract()
    gdf = transform(data)
    truncate()
    msg = load(gdf)
    write_status_to_s3(gdf)
    return msg
