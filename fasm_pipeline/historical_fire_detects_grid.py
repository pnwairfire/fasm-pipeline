"""Historical fire detects grid ingest -> pwfsl_map.fire_detects_historical_grid."""



import logging

import geopandas as gpd
from sqlalchemy import text

from fasm_pipeline import config
from fasm_pipeline.db import get_airfire_engine, get_ts_db_conn, get_ts_engine
from fasm_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)



def extract():
    query_sql = read_sql("query_fire_detects_grid.sql").replace("\n", " ")

    airfire_engine = get_airfire_engine()
    with airfire_engine.begin() as conn:
        gdf = gpd.GeoDataFrame.from_postgis(sql=text(query_sql), con=conn)

    gdf = gdf[["lsd", "last_72hr", "geom"]]
    gdf = gdf.set_geometry("geom")
    gdf = gdf.set_crs("epsg:4326")
    gdf.reset_index(drop=True, inplace=True)

    logger.info(f"EXTRACTED {len(gdf)} detect grid-cells from fire database")
    return gdf


def truncate():
    table = config.qualified(config.FIRE_DETECTS_GRID_TABLE)
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



def load(gdf):
    table = config.qualified(config.FIRE_DETECTS_GRID_TABLE)
    engine = get_ts_engine()
    gdf.to_postgis(config.FIRE_DETECTS_GRID_TABLE, engine, schema=config.DEST_SCHEMA, if_exists="append")
    logger.info(f"LOADED {len(gdf)} grid-cells to {table}")
    return f"Loaded {len(gdf)} historical fire detect grid-cells successfully"


def run():
    """Run the full historical fire detects grid ingest. Returns a summary string."""
    gdf = extract()
    truncate()
    return load(gdf)
