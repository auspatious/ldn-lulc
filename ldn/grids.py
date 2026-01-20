from pathlib import Path

import pandas as pd
import geopandas as gpd
from ldn.utils import ALL_COUNTRIES

from odc.geo.geom import Geometry

from odc.geo.gridspec import GridSpec, XY

from antimeridian import fix_polygon

EPSG_CODE = 6933  # NSIDC EASE-Grid 2.0 Global

GADM_FILE = Path(__file__).parent / "gadm_sids.gpkg"


def get_gadm(
    countries: dict = ALL_COUNTRIES, overwrite: bool = False
) -> gpd.GeoDataFrame:
    if not GADM_FILE.exists() or overwrite:
        all_polys = []
        for _, country_code in countries.items():
            url = f"https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_{country_code}.gpkg"
            country_gdf = gpd.read_file(url, layer="ADM_ADM_0")
            all_polys.append(country_gdf)

        pd.concat(all_polys).to_file(GADM_FILE)

    return gpd.read_file(GADM_FILE)


def get_gridspec(resolution: int = 30, crs: int = EPSG_CODE) -> GridSpec:
    gridspec_origin = XY(-20_000_000.0, -10_000_000.0)

    side_in_meters = 96_000
    shape = (side_in_meters / resolution, side_in_meters / resolution)

    return GridSpec(
        crs=crs,
        tile_shape=shape,
        resolution=resolution,
        origin=gridspec_origin,
    )


def get_all_tiles(
    format: str = "list", countries: dict = ALL_COUNTRIES, overwrite: bool = False
) -> gpd.GeoDataFrame | list:
    """
    Returns a list of all grid tiles (as (x, y) tuples) that cover the combined geometry of all SIDS and DEP countries.

    Alternately, returns a Geopandas GeoDataFrame of the tiles.
    """
    GEOJSON_FILE = Path(__file__).parent / "sids_tiles.geojson"

    if not GEOJSON_FILE.exists() or overwrite:
        grid = get_gridspec()
        gadm = get_gadm(countries=countries)

        all_polys = []

        for country, code in countries.items():
            selection = gadm[gadm["GID_0"] == code]
            if selection.empty:
                raise ValueError(
                    f"No geometry found for country: {country} with code {code}"
                )
            polygon = Geometry(selection.geometry.union_all(), crs=gadm.crs)
            tiles = list(grid.tiles_from_geopolygon(polygon))

            geoboxes = [tile[1] for tile in tiles]
            geobox_labels = [
                list(tile[0]) + [f"{tile[0][0]}_{tile[0][1]}"] for tile in tiles
            ]
            geobox_extents = [fix_polygon(gb.extent.to_crs("epsg:4326")) for gb in geoboxes]

            labels_df = pd.DataFrame(
                geobox_labels, columns=["x_index", "y_index", "label"]
            )
            extents_gdf = gpd.GeoDataFrame(
                labels_df,
                geometry=geobox_extents,
                crs="epsg:4326",
            )
            all_polys.append(extents_gdf)
        extents_gdf = pd.concat(all_polys)
        # Remove duplicates
        extents_gdf = extents_gdf.drop_duplicates(subset=["label"]).reset_index(drop=True)
        # Drop three duplicates. 389_82, 389_81 and 389_79
        extents_gdf = extents_gdf[~extents_gdf["label"].isin(["389_82", "389_81", "389_79"])]
        extents_gdf.to_file(GEOJSON_FILE, driver="GeoJSON")
    else:
        extents_gdf = gpd.read_file(GEOJSON_FILE)

    if format == "list":
        # Return a list of (x, y) tuples
        return [tuple(map(int, label.split("_"))) for label in extents_gdf["label"].tolist()]
    elif format == "gdf":
        return extents_gdf
    else:
        raise ValueError("format must be 'list' or 'gdf'")
