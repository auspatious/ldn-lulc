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
from odc.geo.geobox import GeoBox
from odc.stac import configure_s3_access
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import Item, ItemCollection
from pystac_client import Client as PyStacClient
from rustac import search_sync
from scipy.ndimage import sobel
from typing_extensions import Annotated
from dep_tools.utils import search_across_180, _fix_geometry

from ldn.grids import get_gadm, get_gridspec
from ldn.utils import GEOMAD_VERSION

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


class StacGeoparquetSearcher(Searcher):
    """Search STAC items in a STAC-Geoparquet file using rustac.

    PystacSearcher targets a live STAC API. Our GeoMAD products are indexed
    in a STAC-Geoparquet on S3, so we use rustac.search_sync instead.
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
            raise EmptyCollectionError()

        logger.info(f"Found {len(items)} GeoMAD items")
        return ItemCollection(items)


def load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, and aspect.

    Loads COP-DEM-GLO-30 tiles from MS PC, resamples
    to the target geobox, and derives terrain features.

    Args:
        geobox: Target grid to align the DEM to.

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    dem_items = search_across_180(
        geobox, client, collections=[DEM_COLLECTION]
    )  # Need to search across 180 for Fijian tiles.
    logger.info(f"Found {len(dem_items)} DEM items")
    assert len(dem_items) > 0, "Must find at least 1 DEM item."
    assert len(dem_items) < 30, (
        "No world-spanning DEMs, should be a small number of tiles (~4)."
    )

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
    ds: xr.Dataset, model, probability_threshold: float, nodata_value: int
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
        raise ValueError(
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
    """Processor that scales GeoMAD data, computes indices and terrain, and runs prediction."""

    def __init__(
        self,
        model,
        logger: logging.Logger,
        probability_threshold: float,
        nodata_value: int,
        **kwargs,
    ):
        """Create a LULC prediction processor.

        Args:
            model: Fitted scikit-learn classifier.
            nodata_value: Integer nodata value for output bands.
            probability_threshold: Probability threshold for classification.
            logger: Optional logger instance.
        """
        super().__init__(**kwargs)
        self._model = model
        self._probability_threshold = probability_threshold
        self._nodata_value = nodata_value
        self._logger = logger

    def process(self, input_data: xr.Dataset) -> xr.Dataset:
        """Scale GeoMAD, compute indices, load DEM terrain, and predict LULC.

        Args:
            input_data: GeoMAD dataset loaded by OdcLoader.

        Returns:
            Dataset with classification and lulc_probability bands.
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
        raise ValueError(
            f"Model path must be a '.joblib' file or a URL to a '.joblib' file,"
            f" not {model_path}"
        )

    try:
        joblib_load(model)
        return model
    except Exception as e:
        logger.exception(f"Failed to load model from {model}: {e}")
        raise typer.Exit(code=1)


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
        f"Starting processing. Tile ID: {tile_id}, Year: {datetime}, Region: {region}, Version: {version}."
    )

    # Split by any of [",", "-", "_"] to be robust.
    tile_id_parts = [int(i) for i in re.split(r"[,\-_]", tile_id)]
    if len(tile_id_parts) != 2:
        raise ValueError(
            f"Tile ID must split into 2 integers, got {tile_id_parts}"
            f" from tile_id '{tile_id}'"
        )
    tile_id_tuple: tuple[int, int] = (tile_id_parts[0], tile_id_parts[1])

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
        raise typer.Exit()  # Exit successfully.

    logger.info(
        "Either item does not exist or overwrite is True, proceeding with processing."
    )

    # Search GeoMAD STAC-Geoparquet for this tile's area and year
    logger.info("Defining GeoMAD searcher.")

    searcher = StacGeoparquetSearcher(
        stac_geoparquet_url=GEOMAD_STAC_GEOPARQUET_URL,
        datetime=datetime,
    )

    # Load GeoMAD bands (excluding count) aligned to the tile geobox
    logger.info("Defining ODC Loader.")
    loader = OdcLoader(
        bands=GEOMAD_BANDS,
        chunks={"x": xy_chunk_size, "y": xy_chunk_size},
        fail_on_error=False,  # TODO: Validate this.
    )

    logger.info("Defining LULC Processor.")
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
            id=tile_id,
            area=geobox,
            searcher=searcher,
            loader=loader,
            processor=processor,
            logger=logger,
            stac_creator=stac_creator,
        ).run()
    except EmptyCollectionError:
        logger.exception("No items found for this tile")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.exception(f"Failed to process with error: {e}")
        raise typer.Exit(code=1)
    finally:
        dask_client.close()

    logger.info(
        f"Completed processing. Wrote {len(paths)} items to"
        f" {itempath.stac_path(tile_id, absolute=True)}"
    )


# get_tile_year_geomad_dem_indices and get_buffered_country are now just for the notebooks.
# TODO: Get for a tile. Can clip to buffered country geom (for training data, not clipped for prediction).
# Actually Alex asked to clip prediction to GADM too. TODO: Make sure prediction works with this updated function.
def get_tile_year_geomad_dem_indices(
    tile_id: str,
    year: str,
    country_wgs84_buffered: GeoDataFrame,
    analysis_crs: str,
) -> xr.Dataset:
    """Load Geomedian, GeoMAD, and DEM data for a tile (clipped to country) and compute terrain/spectral features.

    The function loads GeoMAD bands for a tile and year, applies
    Landsat scaling to geomedian bands, derives spectral indices, loads Copernicus
    DEM data aligned to the GeoMAD grid, computes slope and aspect, and clips the
    merged dataset to the tile geometry.

    Args:
        tile_id: The ID of the tile to process.
        year: Temporal filter used for GeoMAD item search.
        country_wgs84_buffered: The (buffered) country to clip the results to.

    Returns:
        An xarray Dataset containing GeoMAD bands, derived spectral indices,
        elevation, slope, and aspect clipped to the tile geometry.
    """
    # tile_x, tile_y = tile_id.split("_")
    # TODO: get_tile_year_geomad_dem_indices has a lot of duplicated logic with the LulcProcessor. Maybe refactor to share code?
    # Needed for Fiji.
    geomad_items = search_sync(
        GEOMAD_STAC_GEOPARQUET_URL,
        ids=f"ausp_ls_geomad_{tile_id}_{year}",
    )

    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    logger.info(
        f"Found {len(geomad_items)} GeoMAD items for tile {tile_id} and year {year}"
    )

    assert len(geomad_items) == 1, (
        f"Must find exactly 1 GeoMAD item for this tile and year, found {len(geomad_items)} instead."
    )

    proj_bbox = geomad_items[0].properties.get("proj:bbox")
    logger.info(proj_bbox)

    bands = [b for b in geomad_items[0].assets.keys() if b != "count"]
    logger.info(f"Available bands (excluding count): {bands}")

    geomad_ds = stac_load(
        geomad_items,
        chunks={},  # Force lazy loading.
        bands=bands,  # Only load the bands we need (exclude count).
        fail_on_error=False,
        geopolygon=country_wgs84_buffered,  # constrain extent (only bbox) - prevents globe-spanning load for antimeridian-crossing tiles.
    )

    assert geomad_ds.odc.crs.epsg == int(analysis_crs.split(":")[1]), (
        f"GeoMAD dataset CRS (EPSG:{geomad_ds.odc.crs.epsg}) does not match analysis CRS ({analysis_crs})"
    )
    logger.info(f"GeoMAD dataset loaded CRS (is native): EPSG:{geomad_ds.odc.crs.epsg}")
    logger.info(f"GeoMAD bands loaded: {list(geomad_ds.data_vars)}")
    logger.info(f"GeoMAD dataset shape: {geomad_ds.dims}")

    geomad_ds = (
        geomad_ds.squeeze()
    )  # .load() # TODO: Why do we load here? Why not keep it lazy?

    # Now clip to tile. DS is shape of country. Clip before DEM search/load for performance.
    geomad_ds = geomad_ds.rio.clip_box(
        minx=proj_bbox[0],
        miny=proj_bbox[1],
        maxx=proj_bbox[2],
        maxy=proj_bbox[3],
        crs=analysis_crs,
    )
    logger.info(f"GeoMAD dataset shape (after tile clip): {geomad_ds.dims}")
    # return geomad_ds, proj_bbox

    # Scale + indices
    geomad_ds = scale_offset_landsat(geomad_ds)

    geomad_ds = calculate_indices(geomad_ds)

    dem_ds = load_dem_terrain(geomad_ds.odc.geobox)

    # Merge GeoMAD (10m native) and DEM (30m, resampled to 10m GeoMAD grid) on x, y, time.
    dem_ds = dem_ds.assign_coords(
        time=geomad_ds.time
    )  # Add GeoMAD time coordinate to DEM dataset so they can be merged.

    merged = xr.merge([geomad_ds, dem_ds])

    # Write NaN as nodata for all bands before clipping (clip fills outside pixels with 0.0 otherwise)
    for var in merged.data_vars:
        merged[var] = merged[var].rio.write_nodata(float("nan"))

    # Do final clip: Make a nice geom of tile and country overlap to clip with.
    # logger.info(merged.odc.geobox.extent.geom.bounds) # (3336000.0, -1888000.0, 3432000.0, -1792000.0)

    country_prj = country_wgs84_buffered.to_crs(merged.odc.geobox.crs)
    # logger.info(country_prj.geometry.total_bounds)

    tile_gdf = GeoDataFrame(
        geometry=[merged.odc.geobox.extent.geom], crs=merged.odc.geobox.crs
    )
    # logger.info(tile_gdf)

    tile_clipped_to_country_gdf = clip(tile_gdf, country_prj)
    # logger.info(tile_clipped_to_country_gdf)
    # logger.info(type(tile_clipped_to_country_gdf))
    # logger.info(tile_clipped_to_country_gdf.total_bounds)

    merged = merged.rio.clip(
        tile_clipped_to_country_gdf.geometry,
        crs=merged.odc.geobox.crs,
        all_touched=True,
        drop=True,
    )

    logger.info(
        f"Merged GeoMAD/DEM dataset shape (after tile+country clip): {merged.dims}"
    )
    return merged


# Dep tools utils have mask_to_gadm() but I want to buffer gadm.
def get_buffered_country(
    country_of_interest: dict, wgs84: str, analysis_crs: str
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
    # _fix_geometry takes Shapely geometry and returns Shapely geometry — no mapping() needed.
    rows = []
    for geom in country_gadm.geometry:
        fixed = _fix_geometry(geom)
        if fixed.geom_type == "MultiPolygon":
            rows.extend(fixed.geoms)  # one row per polygon (east/west of AM)
        else:
            rows.append(fixed)

    return GeoDataFrame(geometry=rows, crs=wgs84)
