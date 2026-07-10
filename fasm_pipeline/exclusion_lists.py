"""Air sensor/monitor exclusion lists ingest.

  PurpleAir -> pwfsl_map.purple_air_exclusion
  Elwood    -> pwfsl_map.elwood_exclusion
"""

try:
    from prefect import task
except ImportError:
    def task(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

import logging

import pandas as pd
from psycopg2.extras import execute_values

from fasm_pipeline import config
from fasm_pipeline.db import get_ts_db_conn
from fasm_pipeline.s3 import airfire_exports_bucket, init_s3

logger = logging.getLogger(__name__)


def get_purple_air_exclusion():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.PURPLE_AIR_EXCLUSION_S3_KEY)
    df = pd.read_json(results["Body"])
    df.rename(columns={0: "unit_id"}, inplace=True)
    logger.info(f"EXTRACTED {len(df)} PurpleAir exclusion records from S3")
    return df


def get_elwood_exclusion():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.ELWOOD_EXCLUSION_S3_KEY)
    df = pd.read_json(results["Body"])
    df.unit_id = df.unit_id.astype(str)
    df.drop(["note", "localTS"], axis=1, inplace=True)
    logger.info(f"EXTRACTED {len(df)} Elwood exclusion records from S3")
    return df


def load_purple_air_exclusion(df):
    table = config.qualified(config.PURPLE_AIR_EXCLUSION_TABLE)
    conn = get_ts_db_conn()
    with conn.cursor() as c:
        c.execute(f"TRUNCATE {table};")
        execute_values(
            cur=c,
            sql=f"""
                INSERT INTO {table}
                (unit_id)
                VALUES %s;
            """,
            argslist=df.to_dict(orient="records"),
            template="(%(unit_id)s)",
        )
        conn.commit()
        c.close()
        conn.close()
    logger.info(f"LOADED {len(df)} records to {table}")
    return f"📋 Loaded {len(df)} PurpleAir exclusion records successfully 📋"


def load_elwood_exclusion(df):
    table = config.qualified(config.ELWOOD_EXCLUSION_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            execute_values(
                cur=c,
                sql=f"""
                    INSERT INTO {table}
                    (unit_id, unit_type)
                    VALUES %s;
                """,
                argslist=df.to_dict(orient="records"),
                template="(%(unit_id)s, %(unit_type)s)",
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"LOADED {len(df)} records to {table}")
    return f"📋 Loaded {len(df)} Elwood exclusion records successfully 📋"


def run():
    """Run both exclusion-list ingests end-to-end. Returns a combined summary."""
    pa_msg = load_purple_air_exclusion(get_purple_air_exclusion())
    elwood_msg = load_elwood_exclusion(get_elwood_exclusion())
    return f"{pa_msg} | {elwood_msg}"
