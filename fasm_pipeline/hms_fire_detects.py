"""HMS fire detects ingest -> pwfsl_map.fire_detects."""

import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from psycopg2.extras import execute_values

from fasm_pipeline import config
from fasm_pipeline.db import get_ts_db_conn
from fasm_pipeline.s3 import airfire_exports_bucket, init_s3

logger = logging.getLogger(__name__)

NORM_COLS = ["latitude", "longitude", "utc_ts", "frp", "source", "satellite"]


def parse_hms_datetime(yearday, time):
    date = datetime.strptime(str(yearday), "%Y%j")
    time_num = float(time)
    hour = int((time_num / 100) // 1)
    minute = int(time_num % 100)
    date = date + timedelta(hours=hour)
    date = date + timedelta(minutes=minute)
    return date


def extract():
    s3 = init_s3()
    results = s3.get_object(Bucket=airfire_exports_bucket(), Key=config.HMS_FIRE_S3_KEY)
    body = results["Body"].read()

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        # The upstream export occasionally writes this file non-atomically,
        # leaving one complete GeoJSON document with a leftover fragment of a
        # previous write appended (json.loads reports this as "Extra data").
        # Recover the first complete document and log the trailing fragment
        # rather than failing the run.
        text = body.decode("utf-8").lstrip()
        payload, end = json.JSONDecoder().raw_decode(text)
        fragment = text[end:]
        logger.warning(
            f"latest_fire.geojson contained a {len(fragment)}-byte fragment "
            f"appended after a complete GeoJSON document (parse error at char "
            f"{e.pos}). The fragment could not be parsed and was discarded; "
            f"proceeding with the first complete document."
        )

    data = payload["features"]
    logger.info(f"EXTRACTED {len(data)} fire detects from S3")
    return data


def transform(data):
    if not data:
        logger.info("No fire detect records returned from extract; returning empty DataFrame.")
        return pd.DataFrame(columns=NORM_COLS)

    # Pull records from the data
    payload = [i.get("properties", {}) for i in data]
    if not payload:
        logger.info("Features contained no properties; returning empty DataFrame.")
        return pd.DataFrame(columns=NORM_COLS)

    raw_df = pd.DataFrame.from_dict(payload, orient="columns")
    if raw_df.empty:
        logger.info("DataFrame created from payload is empty; returning empty DataFrame.")
        return pd.DataFrame(columns=NORM_COLS)

    # Create normalized dataframe with safe column access
    norm_df = pd.DataFrame(columns=NORM_COLS)
    norm_df["latitude"] = raw_df["Lat"] if "Lat" in raw_df.columns else pd.Series(dtype="float64")
    norm_df["longitude"] = raw_df["Lon"] if "Lon" in raw_df.columns else pd.Series(dtype="float64")

    if "YearDay" in raw_df.columns and "Time" in raw_df.columns:
        norm_df["utc_ts"] = raw_df.apply(lambda x: parse_hms_datetime(x.YearDay, x.Time), axis=1)
    else:
        norm_df["utc_ts"] = pd.Series(dtype="datetime64[ns]")

    norm_df["frp"] = raw_df["FRP"] if "FRP" in raw_df.columns else 0
    norm_df["source"] = raw_df["Source"] if "Source" in raw_df.columns else "Unknown"
    norm_df["satellite"] = raw_df["Satellite"] if "Satellite" in raw_df.columns else "Unknown"

    # Detects manually input (source: Analysis) default to FRP of -999, raise to 1
    norm_df.loc[norm_df["frp"] < -1, "frp"] = 1
    norm_df["frp"] = norm_df["frp"].round(2)

    # Round lat/long to 4 decimal places (~4m precision)
    norm_df["latitude"] = norm_df["latitude"].round(4)
    norm_df["longitude"] = norm_df["longitude"].round(4)

    # Remove duplicate points, keeping most recent
    if not norm_df.empty and "utc_ts" in norm_df.columns:
        norm_df = norm_df.sort_values("utc_ts", ascending=True)
        norm_df = norm_df.drop_duplicates(subset=["latitude", "longitude"], keep="last")

    logger.info(f"TRANSFORMED {len(norm_df)} fire detects after deduplication")
    return norm_df


def load(df):
    table = config.qualified(config.FIRE_DETECTS_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            execute_values(
                cur=c,
                sql=f"""
                    INSERT INTO {table}
                    (latitude, longitude, utc_ts, frp, source, satellite)
                    VALUES %s;
                """,
                argslist=df.to_dict(orient="records"),
                template="""
                    (
                        %(latitude)s, %(longitude)s, %(utc_ts)s, %(frp)s,
                        %(source)s, %(satellite)s
                    )
                """,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"LOADED {len(df)} fire detects to {table}")
    return f"🔥 Loaded {len(df)} HMS fire detects successfully 🔥"


def run():
    """Run the full HMS fire detects ingest end-to-end. Returns a summary string."""
    data = extract()
    df = transform(data)
    return load(df)
