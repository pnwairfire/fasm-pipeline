"""Clarity sensors ingest -> pwfsl_map.clarity_sensors."""

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
    "unit_id",
    "latitude",
    "longitude",
    "utc_ts",
    "timezone",
    "raw_pm25",
    "nowcast",
    "site_name",
]


def extract():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.CLARITY_S3_KEY)
    json_data = json.load(results["Body"])
    df = pd.json_normalize(json_data["features"])
    logger.info(f"EXTRACTED {len(df)} Clarity sensor records from S3")
    return df


def normalize(df):
    norm_df = pd.DataFrame(columns=NORM_COLS)
    norm_df.unit_id = df["properties.monitorID"]
    norm_df.latitude = df["geometry.coordinates"].str[1]
    norm_df.longitude = df["geometry.coordinates"].str[0]
    norm_df.utc_ts = df["properties.lastValidUTCTime"]
    norm_df.timezone = df["properties.timezone"]
    norm_df.raw_pm25 = df["properties.PM2.5_1hr"]
    norm_df.nowcast = df["properties.PM2.5_nowcast"]
    norm_df.site_name = df["properties.siteName"]
    logger.info(f"TRANSFORMED (normalized) {len(norm_df)} records")
    return norm_df


def process(df):
    df.raw_pm25 = df.raw_pm25.astype(float).clip(lower=0)
    df.nowcast = df.nowcast.astype(float).clip(lower=0)
    df = df.replace({np.nan: None})
    df["aqi"] = df["nowcast"].apply(pm25_to_aqi)
    df.aqi = df.aqi.astype("Int64", errors="ignore")
    df = df.replace({np.nan: None})
    df = offset_hour(df, 1)
    df = add_latency(df=df)
    df = add_status(df)
    logger.info(f"TRANSFORMED {len(df)} records with AQI, latency, and status")
    return df


def load(df):
    table = config.qualified(config.CLARITY_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            execute_values(
                cur=c,
                sql=f"""
                    INSERT INTO {table}
                    (unit_id, latitude, longitude, utc_ts, timezone, raw_pm25, nowcast,
                     site_name, aqi, latency_mins, status)
                    VALUES %s;
                """,
                argslist=df.to_dict(orient="records"),
                template="""
                    (
                        %(unit_id)s, %(latitude)s, %(longitude)s, %(utc_ts)s,
                        %(timezone)s, %(raw_pm25)s, %(nowcast)s, %(site_name)s,
                        %(aqi)s, %(latency_mins)s, %(status)s
                    )
                """,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"LOADED {len(df)} records to {table}")
    return f"🌫️ Loaded {len(df)} Clarity sensor records successfully 🌫️"


def run():
    """Run the full Clarity ingest end-to-end. Returns a summary string."""
    df = extract()
    df = normalize(df)
    df = process(df)
    return load(df)
