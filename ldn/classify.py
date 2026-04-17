import re
import logging
from pathlib import Path
from typing import Literal

import boto3
import numpy as np
import pandas as pd
import requests
import typer
import xarray as xr
import rioxarray  # noqa: F401 for the .rio accessor
from dask.distributed import Client as DaskClient
from dep_tools.aws import object_exists
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task
from geopandas import GeoDataFrame
from odc.geo.geom import Geometry
from joblib import load as joblib_load
from sklearn.ensemble import RandomForestClassifier
from odc.geo.geobox import GeoBox
from odc.stac import configure_s3_access
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import Item, ItemCollection
from pystac_client import Client as PyStacClient
from rustac import search_sync
from scipy.ndimage import sobel
from typing_extensions import Annotated
from dep_tools.utils import search_across_180, bbox_across_180, _fix_geometry
from rasterio.enums import Resampling
from shapely.geometry import box
from odc.geo.geom import box as odc_box


from ldn.grids import get_gadm, get_gridspec
from ldn.utils import GEOMAD_VERSION, LdnError, get_analysis_epsg

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

    bands_to_scale = [
        band
        for band in data.data_vars
        if band not in ["count", "emad", "smad", "bcmad"]
    ]

    for band in bands_to_scale:
        raw = data[band]
        nodata = (raw == nodata_values[0]) | (raw == nodata_values[1])
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


# GeoMAD output bands that the prediction pipeline needs (excludes "count").
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

GEOMAD_STAC_GEOPARQUET_URL = f"https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/{GEOMAD_VERSION}/ausp_ls_geomad.parquet"

wgs84 = "EPSG:4326"


class StacGeoparquetSearcher(Searcher):
    """Search STAC items in a STAC-Geoparquet file using rustac.

    Searches by tile ID rather than bbox to avoid globe-spanning queries
    for antimeridian-crossing tiles.
    """

    def __init__(self, stac_geoparquet_url: str, datetime: str):
        """Create a searcher for a STAC-Geoparquet file.

        Args:
            stac_geoparquet_url: HTTP(S) URL to the STAC-Geoparquet file.
            datetime: Temporal filter string (e.g. "2020").
        """
        super().__init__()
        self._url = stac_geoparquet_url
        self._datetime = datetime

    def search(self, area: GeoDataFrame | GeoBox) -> ItemCollection:
        """Search for STAC items intersecting the area.

        When the area is a GeoBox, derives the tile ID from the geobox
        and searches by ID to avoid antimeridian wrapping issues.

        Args:
            area: A GeoDataFrame or GeoBox defining the search area.

        Returns:
            A pystac ItemCollection of matching items.
        """
        if isinstance(area, GeoBox):
            bbox = list(area.geographic_extent.boundingbox)
        else:
            bbox = list(area.total_bounds)

        raw = search_sync(self._url, bbox=bbox, datetime=self._datetime)
        items = [Item.from_dict(doc) for doc in raw]

        if len(items) == 0:
            raise LdnError("No GeoMAD items found")

        logger.info(f"Found {len(items)} GeoMAD items")
        return ItemCollection(items)


class GeopolygonOdcLoader(OdcLoader):
    """OdcLoader that uses geopolygon instead of geobox for AM-safe loading.

    The standard OdcLoader passes geobox= to stac_load, which fails for
    antimeridian-crossing tiles. This subclass converts the geobox to an
    AM-fixed WGS84 geopolygon before loading.
    """

    def __init__(self, analysis_crs: Literal["EPSG:3832", "EPSG:6933"], **kwargs):
        """Create a GeopolygonOdcLoader.

        Args:
            analysis_crs: The projected CRS string (either "EPSG:3832" or "EPSG:6933").
            **kwargs: Additional arguments passed to OdcLoader.
        """
        super().__init__(**kwargs)
        self._analysis_crs = analysis_crs

    def load(self, items, areas):
        """Load STAC items using geopolygon instead of geobox.

        Converts the geobox to a WGS84 GeoDataFrame with AM-fixing,
        then delegates to the parent OdcLoader. After loading, crops
        to the original geobox so the output has exact tile dimensions
        (geopolygon-based loading can pull in neighbouring tiles when the
        WGS84 footprint slightly overlaps their extent).

        Args:
            items: The STAC items to load.
            areas: A GeoBox or GeoDataFrame defining the load area.

        Returns:
            The loaded xarray Dataset or DataArray.
        """
        original_geobox = None
        if isinstance(areas, GeoBox):
            original_geobox = areas
            tile_geom = areas.extent.geom
            tile_gdf = GeoDataFrame(geometry=[tile_geom], crs=areas.crs).to_crs(wgs84)
            fixed = _fix_geometry(tile_gdf.geometry.iloc[0])
            areas = GeoDataFrame(geometry=[fixed], crs=wgs84)

        result = super().load(items, areas)

        # Crop to the original geobox extent. geopolygon-based loading
        # may return a larger extent when the WGS84 footprint overlaps
        # neighbouring STAC items at tile boundaries.
        if original_geobox is not None and result.odc.geobox != original_geobox:
            logger.info(
                f"Cropping loaded data from {result.odc.geobox.shape} "
                f"to target geobox {original_geobox.shape}"
            )
            result = result.odc.crop(original_geobox.extent, apply_mask=False)

        return result


def _load_dem_am(
    dem_items: ItemCollection,
    geobox: GeoBox,
    geobox_wgs84: GeoDataFrame,
) -> xr.Dataset:
    """Load DEM for a tile that crosses the antimeridian.

    This is needed to prevent a memory error due to loading a world-spanning DEM tile when the WGS84 footprint overlaps both sides of the AM.
    Can't load straight to target geobox CRS. Must load to WGS84, shift longitudes, concatenate, then reproject with +over.

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
    east_gdf = GeoDataFrame(geometry=[box(*east_bbox)], crs=wgs84)
    west_gdf = GeoDataFrame(geometry=[box(*west_bbox)], crs=wgs84)

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
            f"Expected to load 2 halves of the DEM but got {len(halves)}. Check if the tile geometry is correct and if the DEM items cover the area."
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
        raise LdnError(
            "No DEM items found. COP-DEM-GLO-30 is global so this is unexpected."
        )
    if len(dem_items) >= 10:
        raise LdnError(
            f"Too many DEM items ({len(dem_items)}). Expected ~4, data may be world-spanning."
        )

    geobox_wgs84 = GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(
        wgs84
    )
    crosses_am = isinstance(bbox_across_180(geobox_wgs84), tuple)

    if crosses_am:
        logger.info("Tile crosses the antimeridian, using custom '+over' loading logic")
        dem = _load_dem_am(dem_items, geobox, geobox_wgs84)
    else:
        logger.info(
            "Tile does not cross the antimeridian, using standard loading logic"
        )
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


def search_and_load_geomad_indices_dem(
    tile_id: str,
    year: str,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
    geopolygon: GeoDataFrame,
) -> xr.Dataset:
    """Search, load, scale, and merge GeoMAD bands, spectral indices, and DEM terrain for a tile.
        Supports antimeridian-crossing tiles.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string for the GeoMAD item search (e.g. "2020").
        analysis_crs: The expected CRS of the GeoMAD data (either "EPSG:3832" or "EPSG:6933").
        geopolygon: GeoDataFrame used to constrain the stac_load extent (the country geom).

    Returns:
        Merged dataset with GeoMAD bands, spectral indices, elevation,
        slope, and aspect, clipped to the tile proj:bbox.
    """
    logging.info(
        f"Searching for GeoMAD item for tile {tile_id} and year {year}, using latest version {GEOMAD_VERSION}"
    )
    geomad_items = search_sync(
        GEOMAD_STAC_GEOPARQUET_URL,
        ids=f"ausp_ls_geomad_{tile_id}_{year}",
    )
    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    geomad_items_n = len(geomad_items)
    logger.info(
        f"Found {geomad_items_n} GeoMAD items for tile {tile_id} and year {year}"
    )

    if geomad_items_n != 1:
        raise LdnError(
            f"Must find exactly 1 GeoMAD item for this tile and year, "
            f"found {geomad_items_n} instead."
        )

    proj_bbox = geomad_items[0].properties.get("proj:bbox")
    logger.info(f"proj:bbox = {proj_bbox}")

    bands = [b for b in geomad_items[0].assets.keys() if b != "count"]
    logger.info(f"Loading bands: {bands}")

    geomad_ds = stac_load(
        geomad_items,
        chunks={},  # Force lazy.
        bands=bands,
        fail_on_error=True,  # We control the data so it shouldn't fail.
        geopolygon=geopolygon,
    )

    if geomad_ds.odc.crs.epsg != int(analysis_crs.split(":")[1]):
        raise LdnError(
            f"GeoMAD dataset CRS (EPSG:{geomad_ds.odc.crs.epsg}) "
            f"does not match analysis CRS ({analysis_crs})"
        )
    logger.info(f"GeoMAD CRS: EPSG:{geomad_ds.odc.crs.epsg}")
    logger.info(f"GeoMAD shape: {geomad_ds.dims}")

    geomad_ds = geomad_ds.squeeze()

    # Clip to tile proj:bbox (the dataset may span the full country extent)
    tile_geom = odc_box(
        proj_bbox[0],
        proj_bbox[1],
        proj_bbox[2],
        proj_bbox[3],
        crs=analysis_crs,
    )
    # apply_mask not needed for this box crop.
    geomad_ds = geomad_ds.odc.crop(tile_geom, apply_mask=False)
    logger.info(f"GeoMAD shape (after tile clip): {geomad_ds.dims}")

    geomad_ds = scale_offset_landsat(geomad_ds)
    geomad_ds = calculate_indices(geomad_ds)

    dem_ds = load_dem_terrain(geomad_ds.odc.geobox)

    # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
    # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
    if "spatial_ref" in dem_ds.coords:
        dem_ds = dem_ds.drop_vars("spatial_ref")

    merged = xr.merge([geomad_ds, dem_ds])
    logger.info(f"Merged GeoMAD+DEM shape: {merged.dims}")
    return merged


def reshape_array_to_2d(
    stacked_array: pd.Series,
    template_ds: xr.Dataset,
    original_mask: xr.DataArray,
    nodata_value: int,
) -> xr.DataArray:
    """Reshape a 1D stacked array back to a 2D DataArray.

    Args:
        stacked_array: Flattened prediction or probability values.
        template_ds: Dataset whose y/x coordinates define the output shape.
        original_mask: Boolean mask (True = nodata) applied to the output.

    Returns:
        A 2D uint8 DataArray with the specified nodata_value for nodata pixels.
    """
    array = stacked_array.to_numpy().reshape(template_ds.y.size, template_ds.x.size)
    da = xr.DataArray(
        array, coords={"y": template_ds.y, "x": template_ds.x}, dims=["y", "x"]
    )
    # nodata_value as NoData. Ensure any remaining NaNs are also set to nodata_value before casting.
    da = da.where(~original_mask, nodata_value).fillna(nodata_value)
    return da.astype("uint8")


def probability_binary(
    probability_da: xr.DataArray,
    threshold: int | float,
    nodata_value: int,
) -> xr.DataArray:
    """
    Converts a probability raster into a binary classification raster based on a threshold.

    - Pixels with probability >= threshold are set to 1.
    - Pixels with probability < threshold (but are valid data) are set to 0.
    - Pixels that were originally NoData (NaN) remain NoData (converted to `nodata_value`).

    Parameters:
    - probability_da (xr.DataArray): Input DataArray with probability values (e.g., 0-100).
                                    Expected to have spatial dimensions (e.g., 'x', 'y').
    - threshold (float): The threshold value to apply. Pixels with probability >= threshold
                         will be classified as 1.
    - nodata_value (int): The value to use for NoData.

    Returns:
    - xr.DataArray: A new DataArray with binary classification (1 for above threshold,
                    0 for below threshold, and `nodata_value` for NoData areas).
    """
    mask = probability_da == nodata_value
    above_threshold = probability_da >= threshold

    final_output = xr.where(above_threshold, 1, 0)
    final_output = xr.where(mask, nodata_value, final_output).astype("uint8")

    return final_output


def do_prediction(
    ds: xr.Dataset,
    model: RandomForestClassifier,
    probability_threshold: float,
    nodata_value: int,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """Run random forest prediction and extract target class probability.

    Converts the dataset to a flat observation table, runs the model,
    and reshapes results back to 2D.

    Args:
        ds: Feature dataset with y/x spatial dimensions.
        model: Fitted scikit-learn classifier with predict/predict_proba.
        probability_threshold: Confidence threshold (0-100) for the binary mask.
        nodata_value: Integer nodata value for output bands.

    Returns:
        A (classification, probability, probability_mask) tuple of uint8
        DataArrays with nodata_value for masked pixels.
    """
    stacked = ds.to_array().stack(dims=["y", "x"])

    # Nodata mask: True for pixels where ANY band is NaN.
    nodata_mask = stacked.isnull().any(dim="variable")

    # Build observation table: fill NaN with nodata_value (masked pixels are excluded below).
    obs = stacked.squeeze().fillna(nodata_value).transpose().to_pandas()

    # Validate that all model features are present before reindexing.
    missing = set(model.feature_names_in_) - set(obs.columns)
    if missing:
        raise LdnError(
            f"Dataset is missing features required by the model: {sorted(missing)}"
        )
    obs = obs.reindex(columns=model.feature_names_in_)

    # Flatten the spatial nodata mask to match the observation index.
    valid = ~nodata_mask.values

    full_predictions = pd.Series(nodata_value, index=obs.index, dtype=np.float32)
    full_probabilities = pd.Series(nodata_value, index=obs.index, dtype=np.float32)

    if valid.any():
        valid_df = obs.loc[valid]
        full_predictions.loc[valid] = model.predict(valid_df).astype(np.float32)
        full_probabilities.loc[valid] = (
            model.predict_proba(valid_df).max(axis=1) * 100
        ).astype(np.float32)

    # Reshape back to 2D; nodata_mask stamps nodata_value over masked pixels.
    nodata_mask_2d = nodata_mask.unstack("dims")
    classification_unfiltered = reshape_array_to_2d(
        full_predictions, ds, nodata_mask_2d, nodata_value=nodata_value
    )
    probability = reshape_array_to_2d(
        full_probabilities, ds, nodata_mask_2d, nodata_value=nodata_value
    )
    probability_mask = probability_binary(
        probability, probability_threshold, nodata_value=nodata_value
    )
    # Keep predictions only where probability_mask == 1 (above threshold).
    classification = classification_unfiltered.where(
        probability_mask == 1, nodata_value
    ).astype("uint8")
    return classification, classification_unfiltered, probability


class LulcProcessor(Processor):
    """Processor that scales GeoMAD, computes indices, loads terrain, and predicts."""

    def __init__(
        self,
        model: RandomForestClassifier,
        logger: logging.Logger,
        probability_threshold: float,
        nodata_value: int,
        **kwargs,
    ):
        """Create a LULC prediction processor.

        Args:
            model: Fitted scikit-learn RandomForestClassifier.
            nodata_value: Integer nodata value for output bands.
            probability_threshold: Probability threshold for classification.
            logger: Logger instance.
        """
        super().__init__(**kwargs)
        self._model = model
        self._probability_threshold = probability_threshold
        self._nodata_value = nodata_value
        self._logger = logger

    def process(self, input_data: xr.Dataset) -> xr.Dataset:
        """Scale GeoMAD, compute indices, load DEM terrain, and predict LULC.

        Args:
            input_data: GeoMAD dataset loaded by GeopolygonOdcLoader.

        Returns:
            Dataset with classification and probability bands.
        """
        self._logger.info("Scaling GeoMAD reflectance bands")
        scaled_data = scale_offset_landsat(input_data).squeeze(drop=True)

        self._logger.info("Computing spectral indices")
        data = calculate_indices(scaled_data)

        # Load DEM aligned to the GeoMAD grid
        self._logger.info("Loading DEM and computing terrain features")
        dem_ds = load_dem_terrain(data.odc.geobox)

        # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
        # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
        if "spatial_ref" in dem_ds.coords:
            dem_ds = dem_ds.drop_vars("spatial_ref")

        # Merge GeoMAD features with terrain features
        merged = xr.merge([data, dem_ds])

        # Compute before prediction: sklearn needs eager numpy arrays,
        # and sending a large lazy graph to Dask workers is slow.
        self._logger.info("Computing merged dataset")
        merged = merged.compute()

        self._logger.info("Running prediction")
        classification, classification_unfiltered, probability = do_prediction(
            merged, self._model, self._probability_threshold, self._nodata_value
        )

        output = xr.Dataset(
            {
                "classification": classification,
                "classification_unfiltered": classification_unfiltered,
                "classification_probability": probability,
            }
        )

        for var in output.data_vars:
            output[var].odc.nodata = self._nodata_value
            output[var].attrs["_FillValue"] = self._nodata_value

        return output


def _load_joblib_model(model_path: str):
    """Load a joblib model from a local file or URL.

    Args:
        model_path: Local path or HTTPS URL to a .joblib model file.

    Returns:
        The path to the local model file (verified loadable).

    Raises:
        ValueError: If model_path is not a .joblib file or HTTPS URL.
        typer.Exit: If the downloaded or local model cannot be loaded.
    """
    models_dir = Path("classification/models")

    if model_path.startswith("https://"):
        models_dir.mkdir(parents=True, exist_ok=True)
        model_local = models_dir / model_path.split("/")[-1]
        if not model_local.exists():
            logger.info(f"Downloading model from {model_path} to {model_local}")
            r = requests.get(model_path, timeout=120)
            r.raise_for_status()
            model_local.write_bytes(r.content)
        model = str(model_local)

    elif model_path.endswith(".joblib"):
        logger.info("Model path is a local joblib file, using directly.")
        model = model_path

    else:
        raise LdnError(
            f"Model path must be a '.joblib' file or a URL to a '.joblib' file,"
            f" not {model_path}"
        )

    try:
        joblib_load(model)
        return model
    except Exception as e:
        logger.exception(f"Failed to load model from {model}: {e}")
        raise LdnError(f"Failed to load model from {model}") from e


def run_classify_task(
    tile_id: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    version_geomad: Annotated[str, typer.Option()],
    region: Literal["pacific", "non-pacific"],
    output_bucket: str,
    model_path: str,
    xy_chunk_size: int,
    asset_url_prefix: str | None,
    decimated: bool,
    overwrite: Annotated[bool, typer.Option()],
    probability_threshold: float,
    nodata_value: int,
) -> None:
    """Run LULC prediction for a single tile and year, writing results to S3.

    Uses GeopolygonOdcLoader (geopolygon= with AM-fixing instead of
    geobox=) so antimeridian-crossing tiles load correctly.

    Args:
        tile_id: Grid tile identifier (e.g. "136_142").
        datetime: Year string (e.g. "2020").
        version: Output version string (e.g. "0-0-1").
        version_geomad: Version of the GeoMAD data to use (e.g. "0-0-1").
        region: Grid region, either "pacific" or "non-pacific".
        output_bucket: S3 bucket for output COGs and STAC metadata.
        model_path: Path or URL to the trained joblib model.
        xy_chunk_size: Chunk size in pixels for lazy loading.
        asset_url_prefix: Optional URL prefix for STAC asset hrefs.
        decimated: If True, use 10x lower resolution (for testing).
        overwrite: If True, overwrite existing output.
        probability_threshold: Confidence threshold (0-100) for the binary mask.
        nodata_value: Integer nodata value for output bands.
    """
    logger.info(
        f"Starting processing. Tile ID: {tile_id}, Year: {datetime}, "
        f"Region: {region}, Version: {version}."
    )

    if version_geomad != GEOMAD_VERSION:
        logger.info(
            "Overriding the latest GeoMAD version ({GEOMAD_VERSION}) with the specified version ({version_geomad})."
        )
        geomad_stac_geoparquet_url = GEOMAD_STAC_GEOPARQUET_URL.replace(
            GEOMAD_VERSION, version_geomad
        )
    else:
        geomad_stac_geoparquet_url = GEOMAD_STAC_GEOPARQUET_URL

    # Split by any of [",", "-", "_"] to be robust.
    tile_id_parts = [int(i) for i in re.split(r"[,\-_]", tile_id)]
    if len(tile_id_parts) != 2:
        raise LdnError(
            f"Tile ID must split into 2 integers, got {tile_id_parts}"
            f" from tile_id '{tile_id}'"
        )
    tile_id_tuple: tuple[int, int] = (tile_id_parts[0], tile_id_parts[1])

    analysis_crs = get_analysis_epsg(region)

    logger.info("Getting gridspec and geobox for tile")
    grid = get_gridspec(region)
    geobox = grid.tile_geobox(tile_id_tuple)

    if decimated:
        logger.warning("Decimating geobox by 10x")
        geobox = geobox.zoom_out(10)

    logger.info("Configuring S3 access")
    configure_s3_access(cloud_defaults=True)

    s3_client = boto3.client("s3")

    logger.info("Loading model")
    loaded_model = _load_joblib_model(model_path)

    aws_region_name = boto3.client("s3").head_bucket(Bucket=output_bucket)[
        "BucketRegion"
    ]

    if asset_url_prefix is None:
        asset_url_prefix = (
            f"https://s3.{aws_region_name}.amazonaws.com/{output_bucket}/"
        )

    itempath = S3ItemPath(
        prefix="ausp",
        bucket=output_bucket,
        sensor="ls",
        dataset_id="lulc_prediction",
        version=version,
        time=datetime,
        full_path_prefix=asset_url_prefix,
    )
    stac_url = itempath.stac_path(tile_id)

    if not overwrite and object_exists(output_bucket, stac_url, client=s3_client):
        logger.info(
            f"Item already exists at {itempath.stac_path(tile_id, absolute=True)}"
        )
        raise LdnError(
            f"Item already exists at {itempath.stac_path(tile_id, absolute=True)}"
        )

    logger.info(
        "Either item does not exist or overwrite is True, proceeding with processing."
    )

    searcher = StacGeoparquetSearcher(
        stac_geoparquet_url=geomad_stac_geoparquet_url,
        datetime=datetime,
    )

    # GeopolygonOdcLoader converts the geobox to an AM-fixed WGS84
    # geopolygon before calling stac_load, so AM-crossing tiles work.
    loader = GeopolygonOdcLoader(
        analysis_crs=analysis_crs,
        bands=GEOMAD_BANDS,
        chunks={"x": xy_chunk_size, "y": xy_chunk_size},
        fail_on_error=True,  # We control the geomad data so it shouldn't fail.
    )

    processor = LulcProcessor(
        model=joblib_load(loaded_model),
        nodata_value=nodata_value,
        logger=logger,
        probability_threshold=probability_threshold,
    )

    stac_creator = StacCreator(itempath=itempath, with_raster=True)

    dask_client = DaskClient(n_workers=4, threads_per_worker=16, memory_limit="12GB")
    try:
        logger.info("Started dask client")
        paths = Task(
            itempath=itempath,
            id=tile_id,  # TODO: Check this type
            area=geobox,
            searcher=searcher,
            loader=loader,
            processor=processor,
            logger=logger,
            stac_creator=stac_creator,
        ).run()
    except EmptyCollectionError:
        logger.exception("No items found for this tile")
        raise LdnError("No items found for this tile")
    except Exception as e:
        logger.exception(f"Failed to process with error: {e}")
        raise LdnError(f"Failed to process tile {tile_id}") from e
    finally:
        dask_client.close()

    logger.info(
        f"Completed processing. Wrote {len(paths)} items to"
        f" {itempath.stac_path(tile_id, absolute=True)}"
    )


# get_tile_year_geomad_dem_indices and get_buffered_country are used by the notebooks.
# get_tile_year_geomad_dem_indices uses a lot of the code in search_and_load_geomad_indices_dem, but the training data notebook needs the extra country clipping so they are separate functions.
def get_tile_year_geomad_dem_indices(
    tile_id: str,
    year: str,
    country_wgs84_buffered: GeoDataFrame,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
) -> xr.Dataset:
    """Load GeoMAD + DEM features for a tile, clipped to buffered country.

    Delegates to search_and_load_geomad_indices_dem for the shared search/load/scale/
    indices/DEM logic, then clips to the intersection of the tile extent
    and the buffered country geometry.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Temporal filter used for GeoMAD item search (e.g. "2020").
        country_wgs84_buffered: Buffered country geometry in WGS84.
        analysis_crs: Projected CRS string (e.g. "EPSG:3832").

    Returns:
        Dataset with GeoMAD bands, spectral indices, elevation, slope,
        and aspect, clipped to the tile-country intersection.
    """
    merged = search_and_load_geomad_indices_dem(
        tile_id=tile_id,
        year=year,
        analysis_crs=analysis_crs,
        geopolygon=country_wgs84_buffered,
    )

    # Clip to intersection of tile extent and buffered country
    country_prj = country_wgs84_buffered.to_crs(merged.odc.geobox.crs)
    tile_extent = merged.odc.geobox.extent
    country_union = country_prj.union_all()
    clip_geom = Geometry(
        tile_extent.geom.intersection(country_union),
        crs=merged.odc.geobox.crs,
    )
    merged = merged.odc.crop(clip_geom, apply_mask=True, all_touched=True)

    logger.info(f"Merged GeoMAD/DEM shape (after country clip): {merged.dims}")
    return merged


# Dep tools utils have mask_to_gadm() which would be helpful, but I want to buffer gadm before masking.
def get_buffered_country(
    country_of_interest: dict,
    wgs84: str,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
) -> GeoDataFrame:
    """Fetch and buffer a country geometry for analysis (antimeridian-fixed).

    Retrieves country geometry from GADM, applies country-specific clipping for
    known edge cases (for example antimeridian handling for Fiji), buffers in
    the analysis CRS, and returns the result in WGS84.

    Args:
        country_of_interest: Mapping of country name to country code (single-item
            dictionary expected).
        wgs84: CRS string for output coordinates (EPSG:4326).
        analysis_crs: Projected CRS string used for buffering in meters.

    Returns:
        A GeoDataFrame containing buffered country geometry in `wgs84`.
    """
    buffer_m = 100

    country_gadm = get_gadm(countries=country_of_interest)

    country_gadm = GeoDataFrame(
        geometry=country_gadm.to_crs(analysis_crs).buffer(buffer_m).to_crs(wgs84),
        crs=wgs84,
    )
    # Do antimeridian fix. Needed for Fiji.
    rows = []
    for geom in country_gadm.geometry:
        fixed = _fix_geometry(geom)
        if fixed.geom_type == "MultiPolygon":
            rows.extend(fixed.geoms)  # one row per polygon (east/west of AM)
        else:
            rows.append(fixed)

    return GeoDataFrame(geometry=rows, crs=wgs84)
