"""FASM fire points ingest -> pwfsl_map.fasm_fire_points."""



import logging

import pandas as pd
from psycopg2.extras import execute_values

from fasm_pipeline import config
from fasm_pipeline.db import FIRE_INFO_SEARCH_PATH, get_airfire_db_conn, get_ts_db_conn
from fasm_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)

QUERY_COLUMNS = [
    "fasm_fire_id", "source_id", "latitude", "longitude", "incident_name",
    "start_time", "last_updated", "fire_type", "fire_cause", "fire_behavior",
    "cumulative_acres", "cumulative_ha", "complex_name", "inciweb_url",
    "calfire_url", "canadian_incident_url", "has_perimeter", "perimeter_last_updated",
]



def extract():
    query_sql = read_sql("query_fasm_fire_points.sql").replace("\n", " ")

    conn = get_airfire_db_conn(FIRE_INFO_SEARCH_PATH)
    cursor = conn.cursor()
    cursor.execute(query_sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    logger.info(f"EXTRACTED {len(result)} points from fire database")
    return result



def transform(result):
    df = pd.DataFrame(result, columns=QUERY_COLUMNS)

    # Fix None -> nan coercion
    str_cols = ["incident_name", "complex_name", "fire_cause", "fire_type",
                "fire_behavior", "inciweb_url", "calfire_url", "canadian_incident_url"]
    df[str_cols] = df[str_cols].astype(object).where(df[str_cols].notna(), other=None)

    # Title case string columns
    for col in ["incident_name", "complex_name", "fire_cause", "fire_type", "fire_behavior"]:
        df[col] = df[col].str.title()

    # Format datetimes - convert NaT to None for postgres compatibility
    for col in ["start_time", "last_updated", "perimeter_last_updated"]:
        df[col] = df[col].astype("object").where(df[col].notna(), None)

    logger.info(f"TRANSFORMED {len(df)} fire points")
    return df



def load(df):
    table = config.qualified(config.FIRE_POINTS_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
            execute_values(
                cur=c,
                sql=f"""
                    INSERT INTO {table}
                    (
                        fasm_fire_id, source_id, latitude, longitude, incident_name,
                        start_time, last_updated, fire_type, fire_cause, fire_behavior,
                        cumulative_acres, cumulative_ha, complex_name, inciweb_url,
                        calfire_url, canadian_incident_url, has_perimeter, perimeter_last_updated
                    )
                    VALUES %s;
                """,
                argslist=df.to_dict(orient="records"),
                template="""
                    (
                        %(fasm_fire_id)s, %(source_id)s, %(latitude)s, %(longitude)s,
                        %(incident_name)s, %(start_time)s, %(last_updated)s, %(fire_type)s,
                        %(fire_cause)s, %(fire_behavior)s, %(cumulative_acres)s, %(cumulative_ha)s,
                        %(complex_name)s, %(inciweb_url)s, %(calfire_url)s, %(canadian_incident_url)s,
                        %(has_perimeter)s, %(perimeter_last_updated)s
                    )
                """,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"LOADED {len(df)} points to {table}")
    return f"🔥 Loaded {len(df)} fire points successfully 🔥"


def run():
    """Run the full fire points ingest end-to-end. Returns a summary string."""
    result = extract()
    df = transform(result)
    return load(df)
