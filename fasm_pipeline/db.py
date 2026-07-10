"""PostgreSQL connection/engine helpers for the FASM pipeline.

Two external databases are used:
  - TS_DB      Tile Server — destination for all streams (schema ``pwfsl_map``)
  - AIRFIRE_DB AirFire      — source DB that holds both the ``fire_info`` schema
               (fire points/perimeters/detects) and the ``outlook_v7`` schema
               (smoke outlooks). Same credentials for both; only the
               search_path differs.

Credentials come from environment variables (see ``.example.env``).
"""

import os
from urllib.parse import quote_plus

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

from fasm_pipeline import config

load_dotenv()


REQUIRED_KEYS = {"host", "user", "password", "database"}


def _get_config(env_vars):
    """Resolve a config dict from a mapping of logical keys to env var names.

    env_vars keys:
        - host (required)
        - port (optional, defaults to 5432)
        - user (required)
        - password (required)
        - database (required)
    """
    if not isinstance(env_vars, dict):
        raise ValueError(
            f"env_vars must be a dict mapping keys to env var names. Required keys: {REQUIRED_KEYS}. Optional: port"
        )

    missing_keys = REQUIRED_KEYS - env_vars.keys()
    if missing_keys:
        raise ValueError(
            f"env_vars missing required keys: {missing_keys}. "
            f"Expected format: {{'host': 'ENV_VAR', 'user': 'ENV_VAR', "
            f"'password': 'ENV_VAR', 'database': 'ENV_VAR'}}"
        )

    missing_env = [v for k, v in env_vars.items() if k in REQUIRED_KEYS and not os.getenv(v)]
    if missing_env:
        raise ValueError(f"Missing env var(s): {', '.join(missing_env)}")

    return {
        "host": os.getenv(env_vars["host"]),
        "port": os.getenv(env_vars.get("port"), "5432"),
        "user": os.getenv(env_vars["user"]),
        "password": os.getenv(env_vars["password"]),
        "database": os.getenv(env_vars["database"]),
    }


def get_uri(env_vars, sslmode="require"):
    cfg = _get_config(env_vars)
    pw = quote_plus(cfg["password"])
    uri = f"postgresql://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return f"{uri}?sslmode={sslmode}" if sslmode else uri


def get_conn(env_vars, options=None, connect_timeout=20):
    cfg = _get_config(env_vars)
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=connect_timeout,
        options=options,
    )


def get_engine(env_vars, options=None):
    uri = get_uri(env_vars, sslmode=None)
    connect_args = {"options": options} if options else {}
    return create_engine(uri, connect_args=connect_args)


# Predefined database configs

AIRFIRE_DB = {
    "host": "AIRFIRE_DB_HOST",
    "port": "AIRFIRE_DB_PORT",
    "user": "AIRFIRE_DB_USER",
    "password": "AIRFIRE_DB_PW",
    "database": "AIRFIRE_DB_DATABASE",
}

TS_DB = {
    "host": "TS_DB_HOST",
    "port": "TS_DB_PORT",
    "user": "TS_DB_USER",
    "password": "TS_DB_PW",
    "database": "TS_DB_DATABASE",
}

FIRE_INFO_SEARCH_PATH = f"-c search_path={config.FIRE_SCHEMA},public"
_TS_SEARCH_PATH = f"-c search_path={config.DEST_SCHEMA},public"
_OUTLOOK_SEARCH_PATH = f"-c search_path={config.OUTLOOK_SCHEMA},public"


def get_ts_db_conn():
    return get_conn(TS_DB, options=_TS_SEARCH_PATH)


def get_airfire_db_conn(options=None):
    return get_conn(AIRFIRE_DB, options)


def get_outlook_db_conn():
    # Outlooks live in the AirFire DB under the outlook schema — same
    # credentials as AIRFIRE_DB, just a different search_path.
    return get_conn(AIRFIRE_DB, options=_OUTLOOK_SEARCH_PATH)


def get_ts_engine():
    return get_engine(TS_DB, options=_TS_SEARCH_PATH)


def get_airfire_engine():
    return get_engine(AIRFIRE_DB, options=FIRE_INFO_SEARCH_PATH)
