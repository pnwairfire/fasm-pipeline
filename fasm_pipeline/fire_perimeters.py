"""FASM fire perimeters ingest -> pwfsl_map.fasm_fire_perimeters."""
import logging

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon
from sqlalchemy import text

from fasm_pipeline import config
from fasm_pipeline.db import get_airfire_engine, get_ts_db_conn, get_ts_engine
from fasm_pipeline.sql_util import read_sql

logger = logging.getLogger(__name__)


def extract():
    query_sql = read_sql("query_fasm_fire_perimeters.sql").replace("\n", " ")
    airfire_engine = get_airfire_engine()
    with airfire_engine.begin() as conn:
        gdf = gpd.GeoDataFrame.from_postgis(sql=text(query_sql), con=conn)
    logger.info(f"EXTRACTED {len(gdf)} perimeters from fire database")
    return gdf


def _convert_to_multipolygon(gdf):
    gdf["multipolygons"] = None
    for index, row in gdf.iterrows():
        geometry = row["geom"]
        geometry = geometry.simplify(0.00001, True)
        if geometry.geom_type == "Polygon":
            multipolygon = MultiPolygon([geometry])
            gdf.at[index, "multipolygons"] = multipolygon
        else:
            gdf.at[index, "multipolygons"] = geometry
    gdf = gdf.drop("geom", axis=1)
    gdf = gdf.rename(columns={"multipolygons": "geom"})
    gdf = gdf.set_geometry("geom")
    gdf = gdf.set_crs("epsg:4326")
    return gdf


def transform(gdf):
    gdf = gdf.dissolve(by="fasm_fire_id", aggfunc="max")
    gdf = gdf.sort_values("cumulative_acres", ascending=False).drop_duplicates("incident_name").sort_index()
    gdf = pd.concat([gdf, gdf.bounds], axis=1)
    gdf = _convert_to_multipolygon(gdf)
    gdf.insert(0, "fasm_fire_id", gdf.index)
    gdf.reset_index(drop=True, inplace=True)
    logger.info(f"TRANSFORMED {len(gdf)} perimeters after dissolve and deduplication")
    return gdf


def load(gdf):
    conn = get_ts_db_conn()
    try:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {config.qualified(config.FIRE_PERIMETERS_TABLE)};")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    engine = get_ts_engine()
    gdf.to_postgis(config.FIRE_PERIMETERS_TABLE, engine, schema=config.DEST_SCHEMA, if_exists="append")

    logger.info(f"LOADED {len(gdf)} perimeters to {config.qualified(config.FIRE_PERIMETERS_TABLE)}")
    return f"🔥 Loaded {len(gdf)} fire perimeters successfully 🔥"


def run():
    """Run the full fire perimeters ingest end-to-end. Returns a summary string."""
    gdf = extract()
    gdf = transform(gdf)
    return load(gdf)
