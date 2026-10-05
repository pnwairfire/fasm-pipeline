import os
from fasm_pipeline.db import get_engine, get_uri, TS_DB


def test_get_uri_uses_psycopg2_driver():
    env = {
        "host": "TEST_DB_HOST",
        "port": "TEST_DB_PORT",
        "user": "TEST_DB_USER",
        "password": "TEST_DB_PW",
        "database": "TEST_DB_DATABASE",
    }
    os.environ["TEST_DB_HOST"] = "localhost"
    os.environ["TEST_DB_PORT"] = "5432"
    os.environ["TEST_DB_USER"] = "testuser"
    os.environ["TEST_DB_PW"] = "testpass"
    os.environ["TEST_DB_DATABASE"] = "testdb"

    uri = get_uri(env, sslmode=None)
    assert uri.startswith("postgresql+psycopg2://"), f"Expected postgresql+psycopg2:// scheme, got: {uri}"


def test_get_engine_uses_psycopg2_dialect():
    os.environ["TS_DB_HOST"] = "localhost"
    os.environ["TS_DB_PORT"] = "5432"
    os.environ["TS_DB_USER"] = "testuser"
    os.environ["TS_DB_PW"] = "testpass"
    os.environ["TS_DB_DATABASE"] = "testdb"

    engine = get_engine(TS_DB)
    assert engine.dialect.driver == "psycopg2", f"Expected psycopg2 driver, got: {engine.dialect.driver}"
