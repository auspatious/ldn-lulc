import logging
from typing import Literal

import numpy as np
import xarray as xr
from dep_tools.aws import BaseClient, object_exists
from dep_tools.namers import S3ItemPath
from dep_tools.stac_utils import StacCreator
from dep_tools.utils import bbox_across_180, join_path_or_url, search_across_180
from dep_tools.writers import AwsDsCogWriter
from geopandas import GeoDataFrame
from odc.geo.geobox import GeoBox
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import ItemCollection
from pystac_client import Client as PyStacClient
from rasterio.enums import Resampling
from scipy.ndimage import sobel
from shapely.geometry import box

from ldn.utils import (
    WGS84,
    LdnError,
    get_public_url_base,
)

logger = logging.getLogger(__name__)


def scale_offset_landsat(data: xr.Dataset) -> xr.Dataset:
    """Scale Landsat Collection 2 reflectance values and mask nodata.

    Applies the USGS scaling formula: scaled = raw * 0.0000275 - 0.2,
    clips to [0, 1], and replaces nodata pixels with NaN.

    Modifies the dataset in place and returns it.

    Args:
        data: Input dataset with Landsat integer reflectance bands.
            Expected nodata values are 0 and 65535.

    Returns:
        The same dataset with bands scaled to float32 in [0, 1].
    """
    # Landsat Collection 2 scaling constants (USGS)
    scale_factor = 0.0000275
    offset = -0.2
    nodata_values = (0, 65_535)

    bands_to_scale = [band for band in data.data_vars if band not in ["count", "emad", "smad", "bcmad"]]

    for band in bands_to_scale:
        raw = data[band]
        nodata = (raw == nodata_values[0]) | (raw == nodata_values[1])
        # TODO: Clip to 0.01 instead of 0 so indices still work? Also would clipping to 0 mean that this becomes nodata?
        scaled = (raw * scale_factor + offset).clip(0, 1).astype("float32")
        data[band] = scaled.where(~nodata, other=np.nan)
    return data


def calculate_indices(geomad: xr.Dataset) -> xr.Dataset:
    """Compute spectral indices from scaled geomedian bands.

    Adds index bands to the dataset in place. Division-by-zero cases
    (e.g. when both bands are 0 or NaN) will produce NaN values.

    Args:
        geomad: GeoMedian/GeoMAD dataset containing at least `nir08`, `red`, `green`,
            `blue`, `swir16`, and `swir22` bands (scaled to [0, 1]).

    Returns:
        The same dataset with additional bands: `ndvi`, `ndwi`, `mndwi`,
        `ndti`, `bsi`, `mbi`, `baei`, and `bui`.
    """
    nir = geomad.nir08
    red = geomad.red
    green = geomad.green
    blue = geomad.blue
    swir1 = geomad.swir16
    swir2 = geomad.swir22
    geomad["ndvi"] = (nir - red) / (nir + red)
    geomad["ndwi"] = (green - nir) / (green + nir)
    geomad["mndwi"] = (green - swir1) / (green + swir1)
    geomad["ndti"] = (red - green) / (red + green)
    geomad["bsi"] = ((swir1 + red) - (nir + blue)) / ((swir1 + red) + (nir + blue))
    geomad["mbi"] = ((swir1 - swir2 - nir) / (swir1 + swir2 + nir)) + 0.5
    geomad["baei"] = (red + 0.3) / (green + swir1)
    ndbi = (swir1 - nir) / (swir1 + nir)  # intermediate, not stored
    geomad["bui"] = ndbi - geomad["ndvi"]
    return geomad


def _compute_terrain(dem_da: xr.DataArray) -> xr.Dataset:
    """Compute slope and aspect from an elevation DataArray.

    Uses Sobel filters to estimate terrain gradients. The pixel
    resolution is assumed to be in meters (projected CRS).

    Args:
        dem_da: 2D elevation DataArray with x/y coordinates.

    Returns:
        Dataset with elevation, slope (degrees), and aspect (degrees).
    """
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem_da.x[1] - dem_da.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope = xr.DataArray(
        np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))),
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="slope",
    )
    aspect = xr.DataArray(
        (90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="aspect",
    )

    return xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})


# GeoMAD output bands that the LULC classification pipeline needs (excludes "count").
GEOMAD_BANDS = [
    "nir08",
    "red",
    "green",
    "blue",
    "swir16",
    "swir22",
    "smad",
    "bcmad",
    "emad",
]

# Copernicus DEM collection on MS PC.
DEM_CATALOG = "https://planetarycomputer.microsoft.com/api/stac/v1/"
DEM_COLLECTION = "cop-dem-glo-30"


def _load_dem_am(
    dem_items: ItemCollection,
    geobox: GeoBox,
    geobox_wgs84: GeoDataFrame,
) -> xr.Dataset:
    """Load DEM for a tile that crosses the antimeridian.

    This is needed to prevent a memory error due to loading a world-spanning DEM tile when the WGS84
    footprint overlaps both sides of the AM.
    Can't load straight to target geobox CRS. Must load to WGS84, shift longitudes,
    concatenate, then reproject with +over.

    Loads east and west halves separately in WGS84, shifts west
    longitudes to >180, concatenates, and reprojects to the target
    geobox using the PROJ "+over" flag.

    The "+over" CRS flag tells PROJ to allow longitudes >180 instead
    of wrapping them. This is required because:
    - stac_load(geobox=) cannot reproject WGS84 data across the AM
      into EPSG:3832 (PROJ maps -180 to the wrong side of the projection).
    - odc.reproject does not support +over CRS (returns all NaN).
    - rioxarray's rio.reproject wraps rasterio.warp.reproject, which
      handles +over correctly.

    Related open issues (no upstream fix as of 2025-04):
    - https://github.com/opendatacube/odc-stac/issues/165
    - https://github.com/opendatacube/odc-stac/issues/172
    - https://github.com/opendatacube/odc-geo/issues/208

    Args:
        dem_items: STAC items from search_across_180.
        geobox: Target geobox in the analysis CRS (e.g. EPSG:3832).
        geobox_wgs84: Tile footprint as a WGS84 GeoDataFrame.

    Returns:
        Dataset with a single "elevation" variable in the target CRS.
    """
    east_bbox, west_bbox = bbox_across_180(geobox_wgs84)
    east_gdf = GeoDataFrame(geometry=[box(*east_bbox)], crs=WGS84)
    west_gdf = GeoDataFrame(geometry=[box(*west_bbox)], crs=WGS84)

    east_items = [i for i in dem_items if i.bbox[0] >= 0]
    west_items = [i for i in dem_items if i.bbox[0] < 0]

    halves = []
    for items, gdf in [(east_items, east_gdf), (west_items, west_gdf)]:
        if not items:
            continue
        ds = (
            stac_load(
                items,
                geopolygon=gdf,
                chunks={},  # Force lazy.
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            ).squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
        )
        halves.append(ds)

    if len(halves) != 2:
        raise LdnError(
            f"Expected to load 2 halves of the DEM but got {len(halves)}. Check if the tile geometry is "
            f"correct and if the DEM items cover the area."
        )

    # Shift west longitudes (-180..-179) to (180..181) so the
    # two halves form a continuous longitude range.
    halves[1] = halves[1].assign_coords(longitude=(halves[1].longitude % 360))
    ds_combined = xr.concat(halves, dim="longitude").sortby("longitude")

    # rio is required here because odc.reproject does not support +over.
    ds_combined = ds_combined.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    ds_combined = ds_combined.rio.write_crs("+proj=longlat +datum=WGS84 +over")
    return ds_combined.rio.reproject(
        str(geobox.crs),
        shape=(geobox.height, geobox.width),
        transform=geobox.transform,
        resampling=Resampling.bilinear,
    ).rename({"data": "elevation"})


def load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, and aspect.

    Loads COP-DEM-GLO-30 tiles from Planetary Computer, reprojects
    to the target geobox, and derives terrain features using Sobel
    filters. Handles antimeridian-crossing tiles via _load_dem_am.

    Args:
        geobox: Target grid (of a tile) in the analysis CRS (EPSG:3832 or EPSG:6933).

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    # AM-crossing-safe search.
    dem_items = search_across_180(geobox, client, collections=[DEM_COLLECTION])
    logger.info(f"Found {len(dem_items)} DEM items")

    if len(dem_items) == 0:
        raise LdnError("No DEM items found. COP-DEM-GLO-30 is global so this is unexpected.")
    if len(dem_items) >= 10:
        raise LdnError(f"Too many DEM items ({len(dem_items)}). Expected ~4, data may be world-spanning.")

    geobox_wgs84 = GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(WGS84)
    crosses_am = isinstance(bbox_across_180(geobox_wgs84), tuple)

    if crosses_am:
        logger.info("Tile crosses the antimeridian, using custom '+over' loading logic")
        dem = _load_dem_am(dem_items, geobox, geobox_wgs84)
    else:
        logger.info("Tile does not cross the antimeridian, using standard loading logic")
        dem = (
            stac_load(
                dem_items,
                geobox=geobox,
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
                chunks={},  # Force lazy
            )
            .squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
            .rename({"data": "elevation"})
        )

    # Assign CRS so spatial_ref matches GeoMAD during xr.merge.
    dem = dem.odc.assign_crs(crs=geobox.crs)

    logger.info(f"DEM elevation shape: {dem['elevation'].shape}")

    return _compute_terrain(dem["elevation"])


# This is needed to support Source.Coop prefix.
# PrefixedS3ItemPath needs the key_prefix thing for Source.Coop.
class PrefixedS3ItemPath(S3ItemPath):
    def __init__(self, key_prefix: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.key_prefix = key_prefix.strip("/") if key_prefix else None

    def path(self, item_id, asset_name=None, ext=".tif", absolute=False) -> str:
        relative_path = super().path(item_id, asset_name=asset_name, ext=ext, absolute=False)
        if self.key_prefix:
            relative_path = f"{self.key_prefix}/{relative_path}"
        return (
            join_path_or_url(self.full_path_prefix, relative_path)
            if absolute and self.full_path_prefix is not None
            else relative_path
        )


# A shared function to create tasks for Geomad and Classification.
def build_pipeline_components(
    tile_id_tuple: tuple[int, int],
    year: str,
    version: str,
    bucket: str,
    owner: str,
    dataset_id: Literal["geomad", "lulc"],
    source_coop_prefix: str | None,
    overwrite: bool,
    collection_url_root: str,
    s3_client: BaseClient,
    sensor: str,
) -> tuple[PrefixedS3ItemPath, StacCreator, AwsDsCogWriter] | None:
    """Build shared pipeline components for GeoMAD and classify tasks.

    Returns None if the item already exists and overwrite is False.
    """
    full_path_prefix = get_public_url_base(bucket)

    itempath = PrefixedS3ItemPath(
        key_prefix=source_coop_prefix,
        prefix=owner,
        bucket=bucket,
        sensor=sensor,
        dataset_id=dataset_id,
        version=version,
        time=year,
        full_path_prefix=full_path_prefix,
    )
    stac_document = itempath.stac_path(tile_id_tuple, absolute=True)
    stac_key = itempath.stac_path(tile_id_tuple, absolute=False)

    logger.info(f"Checking if item exists at {stac_document} with overwrite={overwrite}")
    if not overwrite and object_exists(bucket, stac_key):
        logger.info(f"Skipping because item already exists at {stac_document};")
        return None
    logger.info("Either item does not exist or overwrite is True, proceeding with processing.")

    stac_creator = StacCreator(
        collection_url_root=collection_url_root,
        itempath=itempath,
        with_raster=True,
    )
    writer = AwsDsCogWriter(
        itempath,
        write_multithreaded=True,
        client=s3_client,
    )

    return itempath, stac_creator, writer
