"""AIRSIS & WRCC mobile/temporary monitors ingest.

Loads two sources into their own tables:
  AIRSIS -> pwfsl_map.airsis_monitors
  WRCC   -> pwfsl_map.wrcc_monitors
"""

try:
    from prefect import task
except ImportError:
    def task(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

import json
import logging

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from fasm_pipeline import config
from fasm_pipeline.aqi import pm25_to_aqi
from fasm_pipeline.db import get_ts_db_conn
from fasm_pipeline.s3 import airfire_exports_bucket, init_s3
from fasm_pipeline.time_util import add_latency, add_status, offset_hour

logger = logging.getLogger(__name__)

NORM_COLS = [
    "unit_id", "latitude", "longitude", "utc_ts", "timezone",
    "raw_pm25", "nowcast", "site_name", "deployment_type",
    "aqsid", "full_aqsid",
]


@task
def extract(key, source_name):
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=key)
    json_data = json.load(results["Body"])
    df = pd.json_normalize(json_data["features"])
    logger.info(f"EXTRACTED {len(df)} {source_name} records from S3")
    return df


@task
def normalize(df, source_name):
    # An empty feed (pd.json_normalize([]) -> 0 rows, 0 columns) is a
    # legitimate state for these mobile/temporary monitors. Return an empty
    # but correctly-columned frame so process/load can run normally and the
    # table gets truncated to reflect "no current monitors".
    if df.empty:
        logger.warning(f"No {source_name} records in feed — nothing to normalize")
        return pd.DataFrame(columns=NORM_COLS)

    if "properties.deploymentType" not in df.columns.to_list():
        df["properties.deploymentType"] = "Temporary"

    norm_df = pd.DataFrame(columns=NORM_COLS)
    norm_df.unit_id = df["properties.monitorID"]
    norm_df.latitude = df["geometry.coordinates"].str[1]
    norm_df.longitude = df["geometry.coordinates"].str[0]
    norm_df.utc_ts = df["properties.lastValidUTCTime"]
    norm_df.timezone = df["properties.timezone"]
    norm_df.raw_pm25 = df["properties.PM2.5_1hr"]
    norm_df.nowcast = df["properties.PM2.5_nowcast"]
    norm_df.site_name = df["properties.siteName"]
    norm_df.deployment_type = df["properties.deploymentType"]
    norm_df.aqsid = df["properties.AQSID"]
    norm_df.full_aqsid = df["properties.fullAQSID"]

    logger.info(f"TRANSFORMED (normalized) {len(norm_df)} {source_name} records")
    return norm_df


@task
def process(df, source_name):
    df.raw_pm25 = df.raw_pm25.astype(float).clip(lower=0)
    df.nowcast = df.nowcast.astype(float).clip(lower=0)
    df = df.replace({np.nan: None})
    df["aqi"] = df["nowcast"].apply(pm25_to_aqi)
    df.aqi = df.aqi.astype("Int64", errors="ignore")
    df = df.replace({np.nan: None})
    df = offset_hour(df, 1)
    df = add_latency(df=df)
    df = add_status(df)
    logger.info(f"TRANSFORMED {len(df)} {source_name} records with AQI, latency, and status")
    return df


@task
def load(df, table_name, source_name):
    table = config.qualified(table_name)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            if df.empty:
                # Truncate, load nothing: the table now correctly reflects
                # that no monitors are currently deployed for this source.
                logger.warning(
                    f"No {source_name} mobile monitors found — "
                    f"truncated {table}, loaded 0 records"
                )
            else:
                execute_values(
                    cur=c,
                    sql=f"""
                        INSERT INTO {table}
                        (unit_id, latitude, longitude, utc_ts, timezone, raw_pm25, nowcast,
                         site_name, deployment_type, aqsid, full_aqsid, aqi, latency_mins, status)
                        VALUES %s;
                    """,
                    argslist=df.to_dict(orient="records"),
                    template="""
                        (
                            %(unit_id)s, %(latitude)s, %(longitude)s, %(utc_ts)s,
                            %(timezone)s, %(raw_pm25)s, %(nowcast)s, %(site_name)s,
                            %(deployment_type)s, %(aqsid)s, %(full_aqsid)s, %(aqi)s,
                            %(latency_mins)s, %(status)s
                        )
                    """,
                )
                logger.info(f"LOADED {len(df)} records to {table}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if df.empty:
        return f"📡 No {source_name} mobile monitors found — {table_name} cleared 📡"
    return f"📡 Loaded {len(df)} {source_name} records successfully 📡"


def _run_source(key, table_name, source_name):
    df = extract(key, source_name)
    df = normalize(df, source_name)
    df = process(df, source_name)
    return load(df, table_name, source_name)


def run():
    """Run AIRSIS then WRCC ingest end-to-end. Returns a combined summary."""
    msgs = [
        _run_source(config.AIRSIS_S3_KEY, config.AIRSIS_TABLE, "AIRSIS"),
        _run_source(config.WRCC_S3_KEY, config.WRCC_TABLE, "WRCC"),
    ]
    return " | ".join(msgs)
