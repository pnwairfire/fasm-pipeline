"""Externalized configuration for FASM data sources and targets.

Every value here is overridable via an environment variable of the SAME NAME;
the default reproduces the previously-hardcoded behavior exactly. This lets an
operator repoint any S3 source key, S3 output key, destination table, or schema
at deploy time (via the orchestrator's env config / an .env file) without
touching pipeline code.

Buckets, credentials, and S3 endpoints live in ``s3.py``; DB credentials live
in ``db.py``. This module covers object KEYS, table names, and schemas — the
"where data comes from / goes to" that operators tend to change.
"""
import os
import re

from dotenv import load_dotenv

load_dotenv()

# A conservative SQL identifier (schema / table name). Config is
# operator-controlled, but validating it keeps a malformed override from
# silently producing broken or injected SQL when interpolated into a statement.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env(key, default):
    return os.getenv(key, default)


def _ident(key, default):
    val = os.getenv(key, default)
    if not _IDENT.match(val):
        raise ValueError(
            f"Config {key}={val!r} is not a valid SQL identifier "
            f"(expected letters, digits, underscore; no dots or spaces)."
        )
    return val


def qualified(table: str) -> str:
    """Return ``<DEST_SCHEMA>.<table>`` for use in raw SQL."""
    return f"{DEST_SCHEMA}.{table}"


# --- Schemas ---
DEST_SCHEMA = _ident("DEST_SCHEMA", "pwfsl_map")          # all streams write here
FIRE_SCHEMA = _ident("FIRE_SCHEMA", "fire_info")          # fire source (AirFire DB)
OUTLOOK_SCHEMA = _ident("OUTLOOK_SCHEMA", "outlook_v7")   # outlook source

# --- S3 source object keys ---
AIRNOW_S3_KEY = _env("AIRNOW_S3_KEY", "monitoring/v2/latest/geojson/fasm_airnow_PM2.5_latest.geojson")
CLARITY_S3_KEY = _env("CLARITY_S3_KEY", "sensors/v3/PM2.5/latest/geojson/fasm_clarity_PM2.5_latest.geojson")
AIRSIS_S3_KEY = _env("AIRSIS_S3_KEY", "monitoring/v2/latest/geojson/fasm_airsis_PM2.5_latest.geojson")
WRCC_S3_KEY = _env("WRCC_S3_KEY", "monitoring/v2/latest/geojson/fasm_wrcc_PM2.5_latest.geojson")
PURPLE_AIR_S3_KEY = _env("PURPLE_AIR_S3_KEY", "maps/purple_air/v4/pas.csv")
HMS_FIRE_S3_KEY = _env("HMS_FIRE_S3_KEY", "hms/v1/geojson/latest_fire.geojson")
HMS_SMOKE_S3_KEY = _env("HMS_SMOKE_S3_KEY", "hms/v1/geojson/latest_smoke.geojson")
PURPLE_AIR_EXCLUSION_S3_KEY = _env("PURPLE_AIR_EXCLUSION_S3_KEY", "maps/exclusion_lists/purple_air.json")
ELWOOD_EXCLUSION_S3_KEY = _env("ELWOOD_EXCLUSION_S3_KEY", "elwood/exclusion_lists/elwood_exclusion.json")

# --- S3 output object keys ---
OUTLOOKS_OUTPUT_S3_KEY = _env("OUTLOOKS_OUTPUT_S3_KEY", "outlooks/published_outlooks.geojson")
HMS_STATUS_S3_KEY = _env("HMS_STATUS_S3_KEY", "status/hms_status.json")

# --- Destination tables (unqualified; combine with DEST_SCHEMA via qualified()) ---
AIRNOW_TABLE = _ident("AIRNOW_TABLE", "airnow_monitors")
CLARITY_TABLE = _ident("CLARITY_TABLE", "clarity_sensors")
AIRSIS_TABLE = _ident("AIRSIS_TABLE", "airsis_monitors")
WRCC_TABLE = _ident("WRCC_TABLE", "wrcc_monitors")
PURPLE_AIR_TABLE = _ident("PURPLE_AIR_TABLE", "purple_air")
FIRE_POINTS_TABLE = _ident("FIRE_POINTS_TABLE", "fasm_fire_points")
FIRE_PERIMETERS_TABLE = _ident("FIRE_PERIMETERS_TABLE", "fasm_fire_perimeters")
FIRE_DETECTS_TABLE = _ident("FIRE_DETECTS_TABLE", "fire_detects")
FIRE_DETECTS_GRID_TABLE = _ident("FIRE_DETECTS_GRID_TABLE", "fire_detects_historical_grid")
HMS_SMOKE_TABLE = _ident("HMS_SMOKE_TABLE", "hms_smoke_plume")
OUTLOOKS_TABLE = _ident("OUTLOOKS_TABLE", "outlooks")
PURPLE_AIR_EXCLUSION_TABLE = _ident("PURPLE_AIR_EXCLUSION_TABLE", "purple_air_exclusion")
ELWOOD_EXCLUSION_TABLE = _ident("ELWOOD_EXCLUSION_TABLE", "elwood_exclusion")
