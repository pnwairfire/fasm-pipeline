"""PurpleAir sensors ingest -> pwfsl_map.purple_air."""

import io
import logging

import numpy as np
import pandas as pd

from fasm_pipeline import config
from fasm_pipeline.aqi import pm25_to_aqi
from fasm_pipeline.db import get_ts_db_conn
from fasm_pipeline.s3 import airfire_exports_bucket, init_s3
from fasm_pipeline.time_util import add_latency, add_status

logger = logging.getLogger(__name__)


def extract():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.PURPLE_AIR_S3_KEY)
    df = pd.read_csv(results["Body"])
    logger.info(f"Retrieved {len(df)} PurpleAir records from S3")
    return df


def process(df):
    df = df.replace({np.nan: None})
    df["aqi"] = df["epa_nowcast"].apply(pm25_to_aqi)
    df.aqi = df.aqi.astype("Int64", errors="ignore")
    df = df.replace({np.nan: None})
    df = add_latency(df=df)
    df = add_status(df)
    logger.info(f"Processed {len(df)} records with AQI, latency, and status")
    return df


def load(df):
    table = config.qualified(config.PURPLE_AIR_TABLE)
    conn = get_ts_db_conn()

    upload_df = pd.DataFrame(
        {
            "unit_id": df["sensor_index"],
            "latitude": df["latitude"],
            "longitude": df["longitude"],
            "utc_ts": df["utc_ts"],
            "corrected_pm25": df["epa_pm25"],
            "nowcast": df["epa_nowcast"],
            "timezone": df["timezone"],
            "raw_pm25": df["raw_pm25"],
            "aqi": df["aqi"],
            "latency_mins": df["latency_mins"],
            "status": df["status"],
        }
    )

    buffer = io.StringIO()
    upload_df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            c.copy_expert(
                f"""
                COPY {table} (
                    unit_id, latitude, longitude, utc_ts, corrected_pm25,
                    nowcast, timezone, raw_pm25, aqi, latency_mins, status
                ) FROM STDIN WITH (FORMAT csv, NULL '\\N')
                """,
                buffer,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"Inserted {len(df)} records to {table}")
    return f"💜 Loaded {len(df)} PurpleAir records successfully 💜"


def run():
    """Run the full PurpleAir ingest end-to-end. Returns a summary string."""
    df = extract()
    df = process(df)
    return load(df)
