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
from geopandas import GeoDataFrame, clip
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
        then delegates to the parent OdcLoader. After loading, reprojects
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

        # Reproject to the original geobox to ensure exact tile dimensions.
        # geopolygon-based loading may return a larger extent when the WGS84
        # footprint overlaps neighbouring STAC items at tile boundaries.
        if original_geobox is not None and result.odc.geobox != original_geobox:
            logger.info(
                f"Reprojecting loaded data from {result.odc.geobox.shape} "
                f"to target geobox {original_geobox.shape}"
            )
            result = result.odc.reproject(original_geobox)

        return result


def load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, and aspect.

    Loads COP-DEM-GLO-30 tiles from MS PC, resamples
    to the target geobox, and derives terrain features.

    Data is in WGS84.

    For antimeridian-crossing tiles, loads east and west halves
    separately and merges them, since stac_load cannot correctly
    reproject DEM tiles across the antimeridian in one pass.

    Args:
        geobox: Target grid to align the DEM to.

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    geobox_wgs84 = GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(
        wgs84
    )

    dem_items = search_across_180(geobox, client, collections=[DEM_COLLECTION])
    logger.info(f"Found {len(dem_items)} DEM items")
    if len(dem_items) == 0:
        raise LdnError(
            "No DEM items found for the tile. This should not happen since COP-DEM-GLO-30 is global."
        )
    if len(dem_items) >= 10:
        raise LdnError(
            f"Too many DEM items found for the tile ({len(dem_items)}). This should only return a small number of tiles (~4), otherwise the data is probably world-spanning."
        )

    tile_bbox = bbox_across_180(geobox_wgs84)

    if isinstance(tile_bbox, tuple):
        # AM-crossing: load each half in geographic space, shift
        # west longitudes to >180, concatenate, then reproject
        # with +over CRS so PROJ treats >180 as valid.
        east_bbox, west_bbox = tile_bbox
        east_gdf = GeoDataFrame(geometry=[box(*east_bbox)], crs=wgs84)
        west_gdf = GeoDataFrame(geometry=[box(*west_bbox)], crs=wgs84)

        east_items = [i for i in dem_items if i.bbox[0] >= 0]
        west_items = [i for i in dem_items if i.bbox[0] < 0]

        target_crs = str(geobox.crs)
        target_shape = (geobox.height, geobox.width)
        target_transform = geobox.transform

        ds_east = (
            stac_load(
                east_items,
                geopolygon=east_gdf,
                chunks={},
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            )
            .squeeze(drop=True)
            .compute()
            if east_items
            else None
        )

        ds_west = (
            stac_load(
                west_items,
                geopolygon=west_gdf,
                chunks={},
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            )
            .squeeze(drop=True)
            .compute()
            if west_items
            else None
        )

        if ds_east is not None and ds_west is not None:
            ds_west = ds_west.assign_coords(longitude=(ds_west.longitude % 360))
            ds_combined = xr.concat([ds_east, ds_west], dim="longitude").sortby(
                "longitude"
            )
        elif ds_east is not None:
            ds_combined = ds_east
        else:
            ds_combined = ds_west
            ds_combined = ds_combined.assign_coords(
                longitude=(ds_combined.longitude % 360)
            )

        ds_combined = ds_combined.rio.set_spatial_dims(
            x_dim="longitude", y_dim="latitude"
        )
        ds_combined = ds_combined.rio.write_crs("+proj=longlat +datum=WGS84 +over")

        dem = ds_combined.rio.reproject(
            target_crs,
            shape=target_shape,
            transform=target_transform,
            resampling=Resampling.bilinear,
        ).rename({"data": "elevation"})
    else:
        dem = (
            stac_load(
                dem_items,
                geobox=geobox,
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            )
            .squeeze(drop=True)
            .compute()
            .rename({"data": "elevation"})
        )

    # Ensure the DEM carries the canonical target CRS so spatial_ref
    # matches the GeoMAD dataset during xr.merge.
    dem = dem.rio.write_crs(str(geobox.crs))

    dem_da = dem["elevation"]
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem.x[1] - dem.x[0]))

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

    logger.info(f"DEM dataset shape: {dem_da.shape}")

    return xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})


def load_geomad_for_tile(
    tile_id: str,
    year: str,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
    chunks: dict,
    geopolygon: GeoDataFrame,
) -> xr.Dataset:
    """Search, load, scale, index, and merge GeoMAD + DEM for a tile.

    This is the shared loading logic used by both the training notebook
    (get_tile_year_geomad_dem_indices) and the prediction pipeline
    (run_classify_task).

    Searches the GeoMAD STAC-Geoparquet by tile ID (not bbox), loads
    with geopolygon= to avoid globe-spanning loads for AM-crossing
    tiles, clips to the tile proj:bbox, applies Landsat scaling,
    computes spectral indices, loads DEM terrain features, and merges.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string for the GeoMAD item search (e.g. "2020").
        analysis_crs: The expected CRS of the GeoMAD data (either "EPSG:3832" or "EPSG:6933").
        chunks: Dask chunking dict passed to stac_load.
        geopolygon: GeoDataFrame used to constrain the stac_load extent.
            Typically the buffered country geometry in WGS84 for training.

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
        chunks=chunks,
        bands=bands,
        fail_on_error=False,
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
    geomad_ds = geomad_ds.rio.clip_box(
        minx=proj_bbox[0],
        miny=proj_bbox[1],
        maxx=proj_bbox[2],
        maxy=proj_bbox[3],
        crs=analysis_crs,
    )
    logger.info(f"GeoMAD shape (after tile clip): {geomad_ds.dims}")

    geomad_ds = scale_offset_landsat(geomad_ds)
    geomad_ds = calculate_indices(geomad_ds)

    dem_ds = load_dem_terrain(geomad_ds.odc.geobox)

    # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
    # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
    if "spatial_ref" in dem_ds.coords:
        dem_ds = dem_ds.drop_vars("spatial_ref")

    # Align time coordinate so xr.merge works when GeoMAD has a time dim
    if "time" in geomad_ds.coords:
        dem_ds = dem_ds.assign_coords(time=geomad_ds.time)

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

        self._logger.info("Loading data into memory")
        # TODO: Should we delay loading?
        loaded_data = scaled_data.compute()

        self._logger.info("Computing spectral indices")
        data = calculate_indices(loaded_data)

        # Load DEM aligned to the GeoMAD grid
        self._logger.info("Loading DEM and computing terrain features")
        dem_ds = load_dem_terrain(data.odc.geobox)

        # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
        # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
        if "spatial_ref" in dem_ds.coords:
            dem_ds = dem_ds.drop_vars("spatial_ref")

        # Merge GeoMAD features with terrain features
        merged = xr.merge([data, dem_ds])

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
        fail_on_error=False,
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
def get_tile_year_geomad_dem_indices(
    tile_id: str,
    year: str,
    country_wgs84_buffered: GeoDataFrame,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
) -> xr.Dataset:
    """Load GeoMAD + DEM features for a tile, clipped to buffered country.

    Delegates to load_geomad_for_tile for the shared search/load/scale/
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
    merged = load_geomad_for_tile(
        tile_id=tile_id,
        year=year,
        analysis_crs=analysis_crs,
        chunks={},
        geopolygon=country_wgs84_buffered,
    )

    # Write NaN as nodata before clipping (clip fills outside with 0.0 otherwise)
    for var in merged.data_vars:
        merged[var] = merged[var].rio.write_nodata(float("nan"))

    # Clip to intersection of tile extent and buffered country
    country_prj = country_wgs84_buffered.to_crs(merged.odc.geobox.crs)
    tile_gdf = GeoDataFrame(
        geometry=[merged.odc.geobox.extent.geom], crs=merged.odc.geobox.crs
    )
    tile_clipped_to_country_gdf = clip(tile_gdf, country_prj)

    merged = merged.rio.clip(
        tile_clipped_to_country_gdf.geometry,
        crs=merged.odc.geobox.crs,
        all_touched=True,
        drop=True,
    )

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
