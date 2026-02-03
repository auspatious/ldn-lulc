from pathlib import Path
from typing import Literal

import pandas as pd
import geopandas as gpd
from ldn.utils import ALL_COUNTRIES

from odc.geo.geom import Geometry

from odc.geo.gridspec import GridSpec
from odc.geo import XY

from antimeridian import fix_polygon

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
def get_gridspec(resolution: int = 30, crs: int = EPSG_CODE) -> GridSpec:
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


def get_all_tiles(
    format: Literal["list", "gdf"] = "list", countries: dict = ALL_COUNTRIES, overwrite: bool = False
) -> gpd.GeoDataFrame | list:
    """
    Returns a list of all grid tiles (as (x, y) tuples) that cover the combined geometry of all SIDS and DEP countries.

    Alternately, returns a Geopandas GeoDataFrame of the tiles.
    """
    GEOJSON_FILE = Path(__file__).parent / "sids_ci_tiles.geojson"

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
            # TODO: We no longer need to fix the antimeridian for CI grid. DEP grid is the way around this.
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
        # Drop three duplicates. 415_87, 415_88 are un-needed.
        extents_gdf = extents_gdf[~extents_gdf["label"].isin(["415_87", "415_88"])]
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
