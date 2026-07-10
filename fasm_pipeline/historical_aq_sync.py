"""Historical AQ snapshot sync — runs a set of SQL files against the TS DB."""

import logging

from fasm_pipeline.db import get_ts_db_conn
from fasm_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)


def run_sql_file(name: str) -> int:
    """Execute a bundled SQL file and return rows affected."""
    conn = get_ts_db_conn()
    sql = read_sql(name)

    try:
        with conn.cursor() as c:
            c.execute(sql)
            rowcount = c.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return rowcount


def sync_purple_air():
    rowcount = run_sql_file("historical_purple_air.sql")
    logger.info(f"SYNCED purple_air: {rowcount} rows affected")
    return f"💜 purple_air: {rowcount} rows"


def sync_permanent_monitors():
    rowcount = run_sql_file("historical_permanent_monitors.sql")
    logger.info(f"SYNCED permanent_monitors: {rowcount} rows affected")
    return f"🏛️ permanent_monitors: {rowcount} rows"


def sync_airnow_sensors():
    rowcount = run_sql_file("historical_airnow_sensors.sql")
    logger.info(f"SYNCED airnow_sensors: {rowcount} rows affected")
    return f"📡 airnow_sensors: {rowcount} rows"


def sync_clarity_sensors():
    rowcount = run_sql_file("historical_clarity_sensors.sql")
    logger.info(f"SYNCED clarity_sensors: {rowcount} rows affected")
    return f"🌫️ clarity_sensors: {rowcount} rows"


def sync_mobile_monitors():
    rowcount = run_sql_file("historical_mobile_monitors.sql")
    logger.info(f"SYNCED mobile_monitors: {rowcount} rows affected")
    return f"🚚 mobile_monitors: {rowcount} rows"


def run():
    """Run all historical AQ syncs end-to-end. Returns a combined summary."""
    results = [
        sync_purple_air(),
        sync_permanent_monitors(),
        sync_airnow_sensors(),
        sync_clarity_sensors(),
        sync_mobile_monitors(),
    ]
    return "✅ Historical AQ sync complete: " + ", ".join(results)
