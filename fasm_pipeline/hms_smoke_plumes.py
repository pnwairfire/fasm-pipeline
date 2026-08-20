"""HMS smoke plumes ingest -> pwfsl_map.hms_smoke_plume (+ S3 status file, archival & EMF metrics)."""

from datetime import datetime, timezone
import hashlib
import json
import logging
import os

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
    """Extract raw GeoJSON payload from S3 and return (raw_bytes, features, source_metadata)."""
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.HMS_SMOKE_S3_KEY)
    raw_bytes = results["Body"].read()
    sha256_hash = hashlib.sha256(raw_bytes).hexdigest()
    source_last_mod = results.get("LastModified")
    source_last_modified_str = source_last_mod.strftime("%Y-%m-%dT%H:%M:%SZ") if source_last_mod else None

    data = json.loads(raw_bytes.decode("utf-8"))
    features = data.get("features", [])
    logger.info(
        f"Retrieved {len(features)} HMS features from S3 "
        f"(sha256={sha256_hash[:12]}..., source_last_modified={source_last_modified_str})"
    )
    metadata = {
        "sha256_hash": sha256_hash,
        "source_last_modified": source_last_modified_str,
        "content_length": len(raw_bytes),
    }
    return raw_bytes, features, metadata


def archive_raw_geojson_to_s3(raw_bytes: bytes, now_dt: datetime | None = None) -> str | None:
    """Archive immutable raw GeoJSON payload into EPA bucket (fasm-layers)."""
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    timestamp_str = now_dt.strftime("%Y%m%d_%H%M%SZ")
    archive_key = f"{config.HMS_SMOKE_ARCHIVE_PREFIX}/{now_dt.strftime('%Y/%m/%d')}/smoke_{timestamp_str}.geojson"
    s3 = init_epa_s3()
    try:
        s3.put_object(
            Bucket=fasm_layers_bucket(),
            Key=archive_key,
            Body=raw_bytes,
            ContentType="application/geo+json",
        )
        logger.info(f"Archived raw HMS GeoJSON to s3://{fasm_layers_bucket()}/{archive_key}")
        return archive_key
    except Exception as e:
        logger.warning(f"Could not archive raw HMS GeoJSON to S3: {e}")
        return None


def archive_status_to_s3(status: dict, now_dt: datetime | None = None) -> str | None:
    """Archive immutable status metadata snapshot into EPA bucket (fasm-layers)."""
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    timestamp_str = now_dt.strftime("%Y%m%d_%H%M%SZ")
    archive_key = f"{config.HMS_STATUS_ARCHIVE_PREFIX}/{now_dt.strftime('%Y/%m/%d')}/hms_status_{timestamp_str}.json"
    s3 = init_epa_s3()
    try:
        s3.put_object(
            Bucket=fasm_layers_bucket(),
            Key=archive_key,
            Body=json.dumps(status, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Archived HMS status snapshot to s3://{fasm_layers_bucket()}/{archive_key}")
        return archive_key
    except Exception as e:
        logger.warning(f"Could not archive HMS status snapshot to S3: {e}")
        return None


def compute_density_counts(gdf) -> dict[str, int]:
    """Compute count of plumes categorized by smoke density."""
    counts = {"Light": 0, "Medium": 0, "Dense": 0}
    if len(gdf) == 0 or "density" not in gdf.columns:
        return counts
    for d in gdf["density"]:
        d_str = str(d).strip().capitalize()
        if "Light" in d_str or d in (5, "5", 5.0):
            counts["Light"] += 1
        elif "Med" in d_str or d in (16, "16", 16.0, 10, "10", 10.0):
            counts["Medium"] += 1
        elif "Dense" in d_str or d in (27, "27", 27.0):
            counts["Dense"] += 1
        else:
            counts[str(d)] = counts.get(str(d), 0) + 1
    return counts


def compute_satellite_counts(gdf) -> dict[str, int]:
    """Compute count of plumes observed per satellite platform."""
    if len(gdf) == 0 or "satellite" not in gdf.columns:
        return {}
    return {str(k): int(v) for k, v in gdf["satellite"].value_counts().to_dict().items()}


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


def write_status_to_s3(
    last_scan_dt,
    is_fallback=False,
    display_date=None,
    features=0,
    staleness_hours=None,
    sha256_hash=None,
    source_last_modified=None,
    archived_geojson_key=None,
    counts_by_density=None,
    counts_by_satellite=None,
    start_utc_min=None,
    start_utc_max=None,
    end_utc_min=None,
    end_utc_max=None,
    now_dt=None,
):
    """Write current hms_status.json and archive status snapshot to S3."""
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    last_checked = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

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

    def _fmt_dt(dt):
        if dt is None:
            return None
        if isinstance(dt, str):
            return dt
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    status = {
        "last_checked": last_checked,
        "last_scan": last_scan_str,
        "display_date": display_date,
        "is_fallback": is_fallback,
        "features": features,
        "staleness_hours": round(staleness_hours, 2) if staleness_hours is not None else None,
        "sha256_hash": sha256_hash,
        "source_last_modified": source_last_modified,
        "archived_geojson_key": archived_geojson_key,
        "counts_by_density": counts_by_density or {"Light": 0, "Medium": 0, "Dense": 0},
        "counts_by_satellite": counts_by_satellite or {},
        "start_utc_min": _fmt_dt(start_utc_min),
        "start_utc_max": _fmt_dt(start_utc_max),
        "end_utc_min": _fmt_dt(end_utc_min),
        "end_utc_max": _fmt_dt(end_utc_max),
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

    # Also archive the status metadata snapshot
    archived_status_key = archive_status_to_s3(status, now_dt=now_dt)

    logger.info(
        f"Wrote hms_status.json — last_checked: {last_checked}, last_scan: {last_scan_str}, "
        f"display_date: {display_date}, is_fallback: {is_fallback}, features: {features}, "
        f"staleness_hours: {status['staleness_hours']}, sha256: {sha256_hash[:12] if sha256_hash else 'N/A'}, "
        f"archived_status: {archived_status_key}"
    )


def emit_emf_metrics(
    feature_count: int,
    staleness_hours: float | None,
    is_truncated: bool,
    density_counts: dict[str, int] | None = None,
    environment: str | None = None,
) -> None:
    """Emit AWS CloudWatch Embedded Metric Format (EMF) metrics to stdout."""
    if environment is None:
        environment = os.getenv("ENVIRONMENT", os.getenv("ENV", "dev"))

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    dense_count = density_counts.get("Dense", 0) if density_counts else 0
    med_count = density_counts.get("Medium", 0) if density_counts else 0
    light_count = density_counts.get("Light", 0) if density_counts else 0

    emf_payload = {
        "_aws": {
            "Timestamp": now_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": "FASM/Pipeline",
                    "Dimensions": [["Environment", "Stream"]],
                    "Metrics": [
                        {"Name": "PlumeFeatureCount", "Unit": "Count"},
                        {"Name": "PlumeLightCount", "Unit": "Count"},
                        {"Name": "PlumeMediumCount", "Unit": "Count"},
                        {"Name": "PlumeDenseCount", "Unit": "Count"},
                        {"Name": "PlumeAgeHours", "Unit": "None"},
                        {"Name": "PlumeTableTruncated", "Unit": "Count"},
                    ],
                }
            ],
        },
        "Environment": environment,
        "Stream": "hms-smoke-plumes",
        "PlumeFeatureCount": feature_count,
        "PlumeLightCount": light_count,
        "PlumeMediumCount": med_count,
        "PlumeDenseCount": dense_count,
        "PlumeAgeHours": float(staleness_hours) if staleness_hours is not None else 0.0,
        "PlumeTableTruncated": 1 if is_truncated else 0,
    }
    print(json.dumps(emf_payload), flush=True)


def is_overnight_lull(now_dt=None, lull_start_hour=2, lull_end_hour=16):
    """Check if current UTC time falls within the overnight/early-morning satellite lull window (02:00 - 16:00 UTC)."""
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    hour = now_dt.hour
    if lull_start_hour <= lull_end_hour:
        return lull_start_hour <= hour < lull_end_hour
    else:
        return hour >= lull_start_hour or hour < lull_end_hour


def run(staleness_threshold_hours: float | None = None) -> str:
    """Run the full HMS smoke plumes ingest end-to-end with archival & audit logging."""
    if staleness_threshold_hours is None:
        staleness_threshold_hours = config.HMS_SMOKE_STALENESS_HOURS

    now_dt = datetime.now(timezone.utc)
    raw_bytes, features, source_meta = extract()
    archived_key = archive_raw_geojson_to_s3(raw_bytes, now_dt=now_dt)

    gdf = transform(features)
    density_counts = compute_density_counts(gdf)
    satellite_counts = compute_satellite_counts(gdf)

    if len(gdf) > 0:
        start_min = gdf["start_utc"].min() if gdf["start_utc"].notna().any() else None
        start_max = gdf["start_utc"].max() if gdf["start_utc"].notna().any() else None
        end_min = gdf["end_utc"].min() if gdf["end_utc"].notna().any() else None
        end_max = gdf["end_utc"].max() if gdf["end_utc"].notna().any() else None

        truncate()
        msg = load(gdf)

        logger.info(
            f"AUDIT [hms-smoke-plumes]: Ingested {len(gdf)} active plumes. "
            f"Densities: {density_counts}. Satellites: {satellite_counts}. "
            f"Time Range: Start [{start_min} -> {start_max}], End [{end_min} -> {end_max}]. "
            f"Source S3 LastModified: {source_meta.get('source_last_modified')}. "
            f"Payload SHA256: {source_meta.get('sha256_hash')}. "
            f"Archived to: {archived_key}."
        )

        write_status_to_s3(
            last_scan_dt=end_max,
            is_fallback=False,
            display_date=end_max.strftime("%Y-%m-%d") if end_max else None,
            features=len(gdf),
            staleness_hours=0.0,
            sha256_hash=source_meta.get("sha256_hash"),
            source_last_modified=source_meta.get("source_last_modified"),
            archived_geojson_key=archived_key,
            counts_by_density=density_counts,
            counts_by_satellite=satellite_counts,
            start_utc_min=start_min,
            start_utc_max=start_max,
            end_utc_min=end_min,
            end_utc_max=end_max,
            now_dt=now_dt,
        )
        emit_emf_metrics(
            feature_count=len(gdf),
            staleness_hours=0.0,
            is_truncated=False,
            density_counts=density_counts,
        )
        return msg
    else:
        existing_count, max_end_utc = get_existing_table_metadata()
        now_utc = now_dt

        if existing_count > 0 and max_end_utc is not None:
            if max_end_utc.tzinfo is None:
                max_end_utc = max_end_utc.replace(tzinfo=timezone.utc)

            age_seconds = (now_utc - max_end_utc).total_seconds()
            age_hours = round(max(0.0, age_seconds / 3600.0), 2)

            if age_hours <= staleness_threshold_hours:
                logger.info(
                    f"0 features in latest_smoke.geojson, but existing plumes are {age_hours}h old "
                    f"(<= {staleness_threshold_hours}h threshold). Retaining {existing_count} existing active features."
                )
                write_status_to_s3(
                    last_scan_dt=max_end_utc,
                    is_fallback=True,
                    display_date=max_end_utc.strftime("%Y-%m-%d") if max_end_utc else None,
                    features=existing_count,
                    staleness_hours=age_hours,
                    sha256_hash=source_meta.get("sha256_hash"),
                    source_last_modified=source_meta.get("source_last_modified"),
                    archived_geojson_key=archived_key,
                    counts_by_density=density_counts,
                    counts_by_satellite=satellite_counts,
                    now_dt=now_dt,
                )
                emit_emf_metrics(
                    feature_count=existing_count,
                    staleness_hours=age_hours,
                    is_truncated=False,
                    density_counts=density_counts,
                )
                return (
                    f"💨 0 features from NOAA (age {age_hours}h <= {staleness_threshold_hours}h); "
                    f"retained {existing_count} existing HMS smoke plume features 💨"
                )
            else:
                logger.info(
                    f"0 features in latest_smoke.geojson and existing plumes are {age_hours}h old "
                    f"(> {staleness_threshold_hours}h threshold). Truncating table to clear stale plume data."
                )
                truncate()
                write_status_to_s3(
                    last_scan_dt=None,
                    is_fallback=False,
                    display_date=None,
                    features=0,
                    staleness_hours=age_hours,
                    sha256_hash=source_meta.get("sha256_hash"),
                    source_last_modified=source_meta.get("source_last_modified"),
                    archived_geojson_key=archived_key,
                    counts_by_density=density_counts,
                    counts_by_satellite=satellite_counts,
                    now_dt=now_dt,
                )
                emit_emf_metrics(
                    feature_count=0,
                    staleness_hours=age_hours,
                    is_truncated=True,
                    density_counts=density_counts,
                )
                return (
                    f"💨 0 features from NOAA for {age_hours}h (> {staleness_threshold_hours}h); "
                    f"truncated HMS smoke plume table 💨"
                )
        else:
            logger.info("0 features in latest_smoke.geojson and table is already empty.")
            write_status_to_s3(
                last_scan_dt=None,
                is_fallback=False,
                display_date=None,
                features=0,
                staleness_hours=None,
                sha256_hash=source_meta.get("sha256_hash"),
                source_last_modified=source_meta.get("source_last_modified"),
                archived_geojson_key=archived_key,
                counts_by_density=density_counts,
                counts_by_satellite=satellite_counts,
                now_dt=now_dt,
            )
            emit_emf_metrics(
                feature_count=0,
                staleness_hours=None,
                is_truncated=False,
                density_counts=density_counts,
            )
            return "💨 0 features from NOAA; plume table already empty 💨"
