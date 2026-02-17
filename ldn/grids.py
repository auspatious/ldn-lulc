import logging
from pathlib import Path
from typing import Literal

import pandas as pd
import geopandas as gpd
from ldn.utils import ALL_COUNTRIES

from odc.geo.geom import Geometry

from odc.geo.gridspec import GridSpec
from odc.geo import XY

from antimeridian import fix_polygon

from dep_tools.grids import (
    PACIFIC_EPSG,
    grid as dep_grid,
    COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES,
)
from ldn.utils import NON_DEP_COUNTRIES

EPSG_CODE = 6933  # NSIDC EASE-Grid 2.0 Global

GADM_FILE = Path(__file__).parent / "gadm_sids.gpkg"


def get_gadm(
    countries: dict = ALL_COUNTRIES, overwrite: bool = False
) -> gpd.GeoDataFrame:
    """
    Downloads the GADM data for the specified countries if not already cached locally.
    Combines the country geometries into a single GeoDataFrame and saves to a local GeoPackage file.
    """
    if not GADM_FILE.exists() or overwrite:
        all_polys = []
        for _, country_code in countries.items():
            url = f"https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_{country_code}.gpkg"
            country_gdf = gpd.read_file(url, layer="ADM_ADM_0")
            all_polys.append(country_gdf)

        pd.concat(all_polys).to_file(GADM_FILE)

    return gpd.read_file(GADM_FILE)


# This is for the non-pacific countries. All pacific countries are covered by the DEP grid (EPSG:3832).
# Pacific data is seperate because of the antimeridian crossing, and consistency with existing DEP work.
def get_gridspec(resolution: int = 30, crs: int = EPSG_CODE, region: Literal["pacific", "non-pacific"] = "non-pacific") -> GridSpec:
    """
    Returns a GridSpec object.
    Defines a uniform spatial grid (projection, resolution, tile size) across the entire globe.
    This GridSpec covers the entire globe with tiles of 96,000m x 96,000m.
    Each tile is made up of 30m pixels (m is EPSG:6933's unit).
    EPSG:6933 (NSIDC EASE-Grid 2.0 Global) is used as the CRS for consistent area representation.

    This GridSpec provides:
        - Consistent global spatial referencing
        - Efficient storage and access
        - Scalable, tile-based processing
    This enables:
        - Simplified analysis and visualization
        - Seamless mosaicking
        - Globally comparable results
    """

    if region not in ["pacific", "non-pacific"]:
        raise ValueError("Invalid region. Must be 'pacific' or 'non-pacific'.")
    
    if region == "pacific":
        # For the Pacific region, we use the DEP-defined grid
        return dep_grid(resolution, simplify_tolerance=0, crs=PACIFIC_EPSG, return_type="GridSpec")

    # Put the origin at a stable, off-Earth corner so the grid never moves.
    # Prevent the antimeridian from coinciding with tile boundaries.
    gridspec_origin = XY(-20_000_000.0, -10_000_000.0)

    side_in_meters = 90_000
    shape_pixels_y_x = (side_in_meters / resolution, side_in_meters / resolution)

    return GridSpec(
        crs=crs,
        tile_shape=shape_pixels_y_x,
        resolution=resolution,
        origin=gridspec_origin,
    )


def get_grid_tiles(
    format: Literal["list", "gdf"] = "list",
    grids: Literal["all", "pacific", "non-pacific"] = "all",
    overwrite: bool = False,
) -> gpd.GeoDataFrame | list[tuple[tuple[int, int], str]]:
    """
    Returns a list of all grid tiles (as ((x, y), region) tuples) that cover the combined geometry of all SIDS and DEP countries.
    Alternately, returns a Geopandas GeoDataFrame of the tiles.
    Writes two GeoJSONs: one for CI, one for DEP.
    As an optimization, if the GeoJSON files already exist and overwrite=False, it will read from those instead of recalculating the tiles.
    Output list format [((x, y), region), ...] or GeoDataFrame with columns ['x_index', 'y_index', 'label', 'geometry', 'region']
    where label is "x_index_y_index".
    """
    if format not in ["list", "gdf"]:
        raise ValueError("Invalid format. Must be 'list' or 'gdf'.")

    if grids not in ["all", "pacific", "non-pacific"]:
        raise ValueError(
            "Invalid grids value. Must be 'all', 'pacific', or 'non-pacific'."
        )

    logging.info(
        f"Getting all tiles for grids: {grids} with format: {format} and overwrite: {overwrite}"
    )

    geojson_path_non_pacific = Path(__file__).parent / "sids_non_pacific_tiles.geojson"
    geojson_path_pacific = Path(__file__).parent / "sids_pacific_tiles.geojson"
    geojson_path_all = Path(__file__).parent / "sids_all_tiles.geojson"

    def process_grid(region, grid_obj, gadm, countries, geojson_file):
        logging.info(
            f"Processing grid {region} for countries: {list(countries.keys())}"
        )
        if not overwrite and geojson_file.exists():
            logging.info(
                "Reading existing GeoJSON file because overwrite is False and file exists."
            )
            extents_gdf = gpd.read_file(geojson_file)
        else:
            logging.info(
                "Calculating tiles because overwrite is True or file does not exist."
            )
            all_polys = []
            for country, code in countries.items():
                selection = gadm[gadm["GID_0"] == code]
                if selection.empty:
                    raise ValueError(
                        f"No geometry found for country: {country} with code {code}"
                    )
                polygon = Geometry(selection.geometry.union_all(), crs=gadm.crs)
                tiles = list(grid_obj.tiles_from_geopolygon(polygon))
                geoboxes = [tile[1] for tile in tiles]
                geobox_labels = [
                    list(tile[0]) + [f"{tile[0][0]}_{tile[0][1]}"] for tile in tiles
                ]
                geobox_extents = [
                    fix_polygon(gb.extent.to_crs("epsg:4326")) for gb in geoboxes
                ]  # Fix antimeridian crossing geoms.
                labels_df = pd.DataFrame(
                    geobox_labels, columns=["x_index", "y_index", "label"]
                )
                extents_gdf = gpd.GeoDataFrame(
                    labels_df, geometry=geobox_extents, crs="epsg:4326"
                )
                extents_gdf["region"] = region
                all_polys.append(extents_gdf)
            extents_gdf = (
                pd.concat(all_polys)
                .drop_duplicates(subset=["label"])
                .reset_index(drop=True)
            )

            extents_gdf.to_file(
                geojson_file, driver="GeoJSON"
            )  # Just write if overwrite is True or file does not exist.
        return extents_gdf

    # Combine the grids if all are requested, otherwise just return the requested grid.
    # In the requested return format. This ensures the two grids are different.
    grid_configs = []
    if grids in ["all", "pacific"]:
        # Pacific is requested
        grid_configs.append(
            (
                "pacific",
                get_gridspec(region="pacific"),
                get_gadm(countries=DEP_COUNTRIES_AND_CODES),
                DEP_COUNTRIES_AND_CODES,
                geojson_path_pacific,
            )
        )
    if grids in ["all", "non-pacific"]:
        # Non-Pacific is requested
        grid_configs.append(
            (
                "non-pacific",
                get_gridspec(region="non-pacific"),
                get_gadm(countries=NON_DEP_COUNTRIES),
                NON_DEP_COUNTRIES,
                geojson_path_non_pacific,
            )
        )

    grid_dfs = [process_grid(*cfg) for cfg in grid_configs]

    all_tiles_df = pd.concat(grid_dfs)
    if all_tiles_df.empty:
        raise ValueError("No tiles found for the requested grids and countries.")

    all_tiles_gdf = gpd.GeoDataFrame(all_tiles_df, geometry="geometry", crs="epsg:4326")

    if grids == "all" and (overwrite or not geojson_path_all.exists()):
        logging.info(
            "Writing combined GeoJSON file for all tiles because grids is 'all', and (overwrite is True or the file does not exist)."
        )
        all_tiles_gdf.to_file(geojson_path_all, driver="GeoJSON")

    if format == "list":
        return [
            ((int(row["x_index"]), int(row["y_index"])), str(row["region"]))
            for _, row in all_tiles_gdf.iterrows()
        ]
    else:
        return all_tiles_gdf.reset_index(drop=True)
