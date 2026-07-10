"""PurpleAir sensors ingest -> pwfsl_map.purple_air."""

import logging

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

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
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            execute_values(
                cur=c,
                sql=f"""
                    INSERT INTO {table}
                    (
                        unit_id, latitude, longitude, utc_ts, corrected_pm25,
                        nowcast, timezone, raw_pm25, aqi, latency_mins, status
                    )
                    VALUES %s;
                """,
                argslist=df.to_dict(orient="records"),
                template="""
                    (
                        %(sensor_index)s, %(latitude)s, %(longitude)s,
                        %(utc_ts)s, %(epa_pm25)s, %(epa_nowcast)s,
                        %(timezone)s, %(raw_pm25)s, %(aqi)s, %(latency_mins)s, %(status)s
                    )
                """,
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
