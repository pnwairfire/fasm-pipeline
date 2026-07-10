"""Smoke outlooks ingest -> pwfsl_map.outlooks (+ legacy S3 GeoJSON publish)."""

try:
    from prefect import task
except ImportError:
    def task(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

import json
import logging

import geopandas as gpd
from shapely.geometry import shape

from fasm_pipeline import config
from fasm_pipeline.db import get_outlook_db_conn, get_ts_db_conn, get_ts_engine
from fasm_pipeline.s3 import airfire_exports_bucket, init_s3
from fasm_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)


def query_outlooks():
    query_sql = read_sql("query_outlooks_v7.sql")

    conn = get_outlook_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(query_sql)
            geojson = c.fetchone()[0]
    finally:
        conn.close()

    # SQL returns null features when no active outlooks match
    if geojson.get("features") is None:
        geojson["features"] = []

    logger.info(f"EXTRACTED {len(geojson['features'])} outlooks from outlook_v7 DB")
    return geojson


def write_geojson_to_s3(geojson):
    """Stop-gap: keep publishing the GeoJSON to the legacy S3 location so
    downstream consumers still reading from S3 keep working until they're
    migrated to read the TS DB directly."""
    s3 = init_s3()
    s3.put_object(
        Bucket=airfire_exports_bucket(),
        Key=config.OUTLOOKS_OUTPUT_S3_KEY,
        Body=json.dumps(geojson, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"WROTE {len(geojson['features'])} outlooks to s3://.../{config.OUTLOOKS_OUTPUT_S3_KEY}")


@task
def transform(data):
    parsed_data = [[
        item["properties"]["outlook_path"],
        item["properties"]["forecast_date"],
        item["properties"]["author"],
        item["properties"]["region_title"],
        item["properties"]["create_date_utc"],
        shape(item["geometry"]),
    ] for item in data]

    gdf = gpd.GeoDataFrame(
        data=parsed_data,
        columns=["outlook_path", "forecast_date_str", "author", "region_title", "create_date_utc", "geom"],
    )
    gdf = gdf.set_geometry("geom")
    gdf.geom = gdf.geom.set_crs(epsg=4326)

    # Sort by area descending
    gdf = gdf.sort_values(
        by="geom",
        key=lambda s: s.apply(lambda g: g.area),
        ascending=False,
    ).reset_index(drop=True)

    logger.info(f"TRANSFORMED {len(gdf)} outlooks")
    return gdf


def truncate():
    table = config.qualified(config.OUTLOOKS_TABLE)
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table};")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(f"TRUNCATED {table}")


@task
def load(gdf):
    table = config.qualified(config.OUTLOOKS_TABLE)
    engine = get_ts_engine()
    gdf.to_postgis(config.OUTLOOKS_TABLE, engine, schema=config.DEST_SCHEMA, if_exists="append")
    logger.info(f"LOADED {len(gdf)} outlooks to {table}")
    return f"🌤️ Loaded {len(gdf)} outlooks successfully 🌤️"


def run():
    """Run the full outlooks ingest end-to-end. Returns a summary string."""
    geojson = query_outlooks()
    write_geojson_to_s3(geojson)
    gdf = transform(geojson["features"])
    truncate()
    return load(gdf)
