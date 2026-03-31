import re
import logging
from pathlib import Path
from typing import Literal
from zipfile import ZipFile

import boto3
import numpy as np
import pandas as pd
import requests
import typer
import xarray as xr
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
from joblib import load as joblib_load
from odc.geo.geobox import GeoBox
from odc.stac import configure_s3_access
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import Item, ItemCollection
from pystac_client import Client as PyStacClient
from rustac import search_sync
from scipy.ndimage import sobel
from shapely.geometry import box
from typing_extensions import Annotated

from ldn.grids import get_gadm, get_gridspec

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
GEOMAD_BANDS = ["nir08", "red", "green", "blue", "swir16", "swir22", "smad", "bcmad", "emad"]

# Copernicus DEM collection on Element 84 Earth Search.
DEM_CATALOG = "https://earth-search.aws.element84.com/v1/"
DEM_COLLECTION = "cop-dem-glo-30"


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

    Loads COP-DEM-GLO-30 tiles from Element 84 Earth Search, resamples
    to the target geobox, and derives terrain features.

    Args:
        geobox: Target grid to align the DEM to.

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    catalog = PyStacClient.open(DEM_CATALOG)
    bbox = list(geobox.geographic_extent.boundingbox)

    # TODO: For antimeridian-crossing tiles like Pacific 66_22 (Fiji) this gives "Found 235 DEM items". Need to fix this like I did for CSDR. It works decimated but is inneficient.
    dem_items = list(catalog.search(collections=[DEM_COLLECTION], bbox=bbox).items())
    logger.info(f"Found {len(dem_items)} DEM items")

    dem = (
        stac_load(
            dem_items,
            geobox=geobox,
            resampling="bilinear",
            patch_url=sign_url,
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

    return xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})


def reshape_array_to_2d(
    stacked_array: pd.Series, template_ds: xr.Dataset, original_mask: xr.DataArray
) -> xr.DataArray:
    """Reshape a 1D stacked array back to a 2D DataArray.

    Args:
        stacked_array: Flattened prediction or probability values.
        template_ds: Dataset whose y/x coordinates define the output shape.
        original_mask: Boolean mask (True = nodata) applied to the output.

    Returns:
        A 2D uint8 DataArray with 255 for nodata pixels.
    """
    array = stacked_array.to_numpy().reshape(template_ds.y.size, template_ds.x.size)
    da = xr.DataArray(
        array, coords={"y": template_ds.y, "x": template_ds.x}, dims=["y", "x"]
    )
    # 255 as NoData.
    return da.where(~original_mask, 255).astype("uint8")


def probability_binary(
    probability_da: xr.DataArray,
    threshold: int | float,
    nodata_value: int = 255,
) -> xr.DataArray:
    """
    Converts a probability raster into a binary classification raster based on a threshold.

    - Pixels with probability >= threshold are set to 1.
    - Pixels with probability < threshold (but are valid data) are set to 0.
    - Pixels that were originally NoData (NaN) remain NoData (converted to `nodata_value`
      if `output_dtype` is an integer type).

    Parameters:
    - probability_da (xr.DataArray): Input DataArray with probability values (e.g., 0-100).
                                    Expected to have spatial dimensions (e.g., 'x', 'y').
    - threshold (float): The threshold value to apply. Pixels with probability >= threshold
                         will be classified as 1.
    - output_dtype (str): The desired output data type. Use 'float32' or 'float64'
                          to preserve NaN values. If an integer type (e.g., 'uint8', 'int16'),
                          original NaNs will be converted to `nodata_value`.
    - nodata_value (int): The value to use for NoData if output_dtype is an integer type.
                          Must be within the range of the chosen integer `output_dtype`.
                          Default is 255.

    Returns:
    - xr.DataArray: A new DataArray with binary classification (1 for above threshold,
                    0 for below threshold, and `nodata_value` for NoData areas).
    """
    mask = probability_da == 255
    above_threshold = probability_da >= threshold

    final_output = xr.where(above_threshold, 1, 0)
    final_output = xr.where(mask, nodata_value, final_output).astype("uint8")

    return final_output


def do_prediction(ds: xr.Dataset, model, #probability_threshold
                  ) -> tuple[xr.DataArray, xr.DataArray]:
    """Run random forest prediction and extract target class probability.

    Converts the dataset to a flat observation table, runs the model,
    and reshapes results back to 2D.

    Args:
        ds: Feature dataset with y/x spatial dimensions.
        model: Fitted scikit-learn classifier with predict/predict_proba.

    Returns:
        A (classification, probability) tuple of uint8 DataArrays
        with 255 as nodata.
    """
    # Store the original nodata mask
    # TODO: Check if any band is nan.
    mask = ds.red.isnull()

    # Convert to a stacked array of observations
    stacked_arrays = ds.to_array().stack(dims=["y", "x"])

    # Replace infinities with NaN
    # TODO: Fix infinity values upstream. They shouldn't be here.
    stacked_arrays = stacked_arrays.where(stacked_arrays != float("inf"))
    stacked_arrays = stacked_arrays.where(stacked_arrays != float("-inf"))

    # Replace NaN with 0 and transpose to (pixels, bands)
    # TODO: Why replace nans?? Zeroes are misleading.
    stacked_arrays = stacked_arrays.squeeze().fillna(0).transpose().to_pandas()

    # Reorder columns to match the feature names the model was trained with.
    stacked_arrays = stacked_arrays.reindex(columns=model.feature_names_in_)

    # Skip all-zero rows (masked areas) for performance
    zero_mask = (stacked_arrays == 0).all(axis=1)
    non_zero_df = stacked_arrays.loc[~zero_mask]

    full_predictions = pd.Series(np.nan, index=stacked_arrays.index)
    full_probabilities = pd.Series(np.nan, index=stacked_arrays.index)

    if not non_zero_df.empty:
        predictions = model.predict(non_zero_df)
        full_predictions.loc[~zero_mask] = predictions

        # Max probability across all classes = confidence of the predicted class.
        probabilities = model.predict_proba(non_zero_df)
        max_probabilities = probabilities.max(axis=1) * 100
        full_probabilities.loc[~zero_mask] = max_probabilities
        # TODO: Mask with probability_binary() and probability_threshold.

    predicted = reshape_array_to_2d(full_predictions, ds, mask)
    probability = reshape_array_to_2d(full_probabilities, ds, mask)

    return predicted, probability


class LulcProcessor(Processor):
    """Processor that scales GeoMAD data, computes indices and terrain, and runs prediction."""

    def __init__(
        self,
        model,
        logger: logging.Logger,
        probability_threshold: float = 60,
        nodata_value: int = 255,
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
        # self._probability_threshold = probability_threshold
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
        classification, probability = do_prediction(
            merged, self._model, # probability_threshold
        )

        output = xr.Dataset(
            {
                "classification": classification,
                "lulc_probability": probability,
                # TODO: Do we need threshold here?
            }
        )

        for var in output.data_vars:
            output[var].odc.nodata = self._nodata_value
            output[var].attrs["_FillValue"] = self._nodata_value

        return output


def _load_joblib_model(model_path: str):
    """Load a joblib model from a local file, URL, or zip archive.

    Args:
        model_path: Path or URL to the model file.

    Returns:
        The path to the local model file (verified loadable).
    """
    if model_path.startswith("https://"):
        model_local = "classification/models/" + model_path.split("/")[-1]
        if not Path(model_local).exists():
            logger.info(f"Downloading model from {model_path} to {model_local}")
            r = requests.get(model_path)
            with open(model_local, "wb") as f:
                f.write(r.content)
        model = model_local

    elif model_path.endswith(".zip"):
        unzipped = "classification/models/" + model_path.split("/")[-1].replace(".zip", "")
        if not Path(unzipped).exists():
            logger.info("Unzipping model")
            with ZipFile(model_path, "r") as zip_ref:
                zip_ref.extractall(path="classification/models/")
        model = unzipped
        logger.info(f"Unzipped model to {model}")

    elif model_path.endswith(".joblib"):
        logger.info("Model path is a local joblib file, using directly.")
        model = model_path

    else:
        raise ValueError(
            f"Model path must be a '.joblib' file, a URL to a '.joblib' file,"
            f" or a '.zip' file, not {model_path}"
        )

    try:
        joblib_load(model)
        return model
    except Exception as e:
        logger.exception(f"Failed to load model from {model}: {e}")
        raise typer.Exit(code=1)


def run_predict_task(
    tile_id: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    version_geomad: Annotated[str, typer.Option()],
    region: Literal["pacific", "non-pacific"],
    output_bucket: str = "data.ldn.auspatious.com",
    model_path: str = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/lulc_random_forest_model.joblib",
    xy_chunk_size: int = 1024,
    asset_url_prefix: str | None = None,
    decimated: bool = False,
    overwrite: Annotated[bool, typer.Option()] = False,
    # probability_threshold,
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
    """
    logger.info(f"Starting processing. Tile ID: {tile_id}, Year: {datetime}, Region: {region}, Version: {version}.")

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

    aws_region_name = boto3.client("s3").head_bucket(Bucket=output_bucket)["BucketRegion"]

    if asset_url_prefix is None:
        asset_url_prefix = f"https://s3.{aws_region_name}.amazonaws.com/{output_bucket}/"

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
        logger.info(f"Item already exists at {itempath.stac_path(tile_id, absolute=True)}")
        raise typer.Exit() # Exit successfully.
    
    logger.info("Either item does not exist or overwrite is True, proceeding with processing.")

    # Search GeoMAD STAC-Geoparquet for this tile's area and year
    logger.info("Defining GeoMAD searcher.")

    stac_geoparquet_url = f"https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/{version_geomad}/ausp_ls_geomad.parquet"
    searcher = StacGeoparquetSearcher(
        stac_geoparquet_url=stac_geoparquet_url,
        datetime=datetime,
    )

    # Load GeoMAD bands (excluding count) aligned to the tile geobox
    logger.info("Defining ODC Loader.")
    loader = OdcLoader(
        bands=GEOMAD_BANDS,
        chunks={"x": xy_chunk_size, "y": xy_chunk_size},
        fail_on_error=False, #TODO: Validate this.
    )

    logger.info("Defining LULC Processor.")
    processor = LulcProcessor(
        model=joblib_load(loaded_model),
        nodata_value=255,
        logger=logger,
        # probability_threshold=probability_threshold,
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









# get_geomad_dem_indices and get_buffered_country are now just for the notebooks.
def get_geomad_dem_indices(region_polygon_gdf: GeoDataFrame, stac_geoparquet: str, year: str, catalog: PyStacClient) -> xr.Dataset:
    """Load Geomedian, GeoMAD, and DEM data for a region and compute terrain/spectral features.

    The function loads GeoMAD bands for a regional AOI and year, applies
    Landsat scaling to geomedian bands, derives spectral indices, loads Copernicus
    DEM data aligned to the GeoMAD grid, computes slope and aspect, and clips the
    merged dataset to the region geometry.

    Args:
        region_polygon_gdf: A GeoDataFrame containing exactly one polygon or
            multipolygon in WGS84 coordinates.
        stac_geoparquet: Path or URL to the STAC geoparquet source used by
            `rustac.search_sync` for GeoMAD item lookup.
        year: Temporal filter used for GeoMAD item search.
        catalog: STAC client used to query DEM items.

    Returns:
        An xarray Dataset containing GeoMAD bands, derived spectral indices,
        elevation, slope, and aspect clipped to the region geometry.
    """
    assert len(region_polygon_gdf.geometry) == 1, "region_polygon_gdf must contain at one multipolygon"

    logger.info(region_polygon_gdf.geometry[0].bounds)

    geomad_items = search_sync(stac_geoparquet, bbox=list(region_polygon_gdf.total_bounds), datetime=year)

    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    logger.info(f"Found {len(geomad_items)} GeoMAD items for this region and year")

    bands = [b for b in geomad_items[0].assets.keys() if b != "count"]
    logger.info(f"Available bands (excluding count): {bands}")

    geomad_ds = stac_load(
        geomad_items,
        # Region is in 4326 which is good for clipping, despite GeoMAD being in 3857 (for pacific region).
        geopolygon=region_polygon_gdf.geometry[0], # Filters but doesn't clip to the region polygon.
        chunks={}, # Force lazy loading.
        bands=bands, # Only load the bands we need (exclude count).
    )

    logger.info(f"GeoMAD dataset loaded CRS (should be native): {geomad_ds.odc.crs.epsg}")
    logger.info(f"GeoMAD bands loaded: {list(geomad_ds.data_vars)}")
    geomad_ds = geomad_ds.squeeze().load()
    logger.info(f"GeoMAD dataset shape: {geomad_ds.dims}")

    # Scale + indices
    geomad_ds = scale_offset_landsat(geomad_ds)

    geomad_ds = calculate_indices(geomad_ds)

    # Now for DEM data do per bbox search and load to avoid loading the whole world for Fiji.
    dem_items = catalog.search(
        collections=["cop-dem-glo-30"],
        bbox=list(region_polygon_gdf.geometry[0].bounds),
        # datetime="2021"
    )
    dem_items = list(dem_items.items())
    logger.info(f"Found {len(dem_items)} DEM items for this AOI")

    dem = stac_load(
        dem_items,
        like=geomad_ds, # Needed for alingment.
        resampling="bilinear", # Alternatively nearest. # TODO: Validate resampling method for upsampling DEM.
        patch_url=sign_url,
    ).squeeze().compute().rename({"data": "elevation"}) # Squeeze removes the time dimension, which is not needed for DEM.

    dem_da = dem['elevation']
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem.x[1] - dem.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope  = xr.DataArray(np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))), coords=dem_da.coords, dims=dem_da.dims, name="slope")
    aspect = xr.DataArray((90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,  coords=dem_da.coords, dims=dem_da.dims, name="aspect")

    dem_ds = xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})

    # Merge GeoMAD (10m native) and DEM (30m, resampled to 10m GeoMAD grid) on x, y, time.
    dem_ds = dem_ds.assign_coords(time=geomad_ds.time) # Add GeoMAD time coordinate to DEM dataset so they can be merged.

    merged = xr.merge([geomad_ds, dem_ds])

    # Write NaN as nodata for all bands before clipping (clip fills outside pixels with 0.0 otherwise)
    for var in merged.data_vars:
        merged[var] = merged[var].rio.write_nodata(float("nan"))

    # Clip.
    return merged.rio.clip(region_polygon_gdf.to_crs(merged.odc.crs).geometry, merged.rio.crs, drop=True)


def get_buffered_country(country_of_interest: dict, wgs84: str, analysis_crs: str) -> GeoDataFrame:
    """Fetch and buffer a country geometry for analysis.

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
    buffer_m  = 100

    country_gadm = get_gadm(countries=country_of_interest, overwrite=True)

    # Temporarily clip country geoms to GeoMAD processed areas because we don't have that much processed yet.
    # TODO: Remove this step once GeoMAD has been run for all tiles for all countries.
    stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/0-0-2b/ausp_ls_geomad.parquet"
    geomad_items = search_sync(stac_geoparquet, bbox=list(country_gadm.total_bounds), datetime="2020")
    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    print(f"Found {len(geomad_items)} GeoMAD items for this country in 2020. Clipping country to the first item while developing.")
    geomad_bbox = geomad_items[0].bbox
    # For Fiji 2nd tile, use a different item.
    # geomad_bbox = geomad_items[1].bbox # TODO: Fix this bbox. Bounds are -179.999995, -16.82498, 180.0, -16.079684.

    country_gadm = country_gadm.clip(box(*geomad_bbox))

    # Buffer country polygon to include coastal zones.
    # Fiji and Singapore are both a single multipolygon from GADM.
    return GeoDataFrame(
        geometry=country_gadm.to_crs(analysis_crs).buffer(buffer_m).to_crs(wgs84),
        crs=wgs84
    )
