import logging
from typing import Literal
import xarray as xr
from odc.stac import load
from pystac import Item
from pystac_client import Client as PyStacClient
from rustac import search_sync
import numpy as np
from planetary_computer import sign_url
from geopandas import GeoDataFrame
from scipy.ndimage import sobel
from shapely.geometry import box
from ldn.grids import get_gadm, get_gridspec

logger = logging.getLogger(__name__)

from pathlib import Path
import re
from zipfile import ZipFile

import boto3
from joblib import load as joblib_load
import requests
import typer
from dask.distributed import Client as DaskClient
from dep_tools.aws import object_exists
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.searchers import PystacSearcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task
from dep_tools.utils import get_logger
from odc.stac import configure_s3_access
from typing_extensions import Annotated


from logging import Logger, getLogger
logger = getLogger(__name__)

from dep_tools.processors import Processor



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


# TODO: Refactor this to use PystacSearcher, OdcLoader
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

    geomad_ds = load(
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

    dem = load(
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

    country_name = list(country_of_interest.keys())[0]

    # Temporarily clip country geoms to GeoMAD processed areas because we don't have that much processed yet.
    # TODO: Remove this step once GeoMAD has been run for all countries. 
    stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet"
    if country_name == "Singapore":
        stac_geoparquet = "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ci_ls_geomad/0-0-2/ci_ls_geomad.parquet"
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


# def probability_binary(
#     probability_da: xr.DataArray,
#     threshold: int | float,
#     nodata_value: int = 255,
# ) -> xr.DataArray:
#     """
#     Converts a probability raster into a binary classification raster based on a threshold.

#     - Pixels with probability >= threshold are set to 1.
#     - Pixels with probability < threshold (but are valid data) are set to 0.
#     - Pixels that were originally NoData (NaN) remain NoData (converted to `nodata_value`
#       if `output_dtype` is an integer type).

#     Parameters:
#     - probability_da (xr.DataArray): Input DataArray with probability values (e.g., 0-100).
#                                     Expected to have spatial dimensions (e.g., 'x', 'y').
#     - threshold (float): The threshold value to apply. Pixels with probability >= threshold
#                          will be classified as 1.
#     - output_dtype (str): The desired output data type. Use 'float32' or 'float64'
#                           to preserve NaN values. If an integer type (e.g., 'uint8', 'int16'),
#                           original NaNs will be converted to `nodata_value`.
#     - nodata_value (int): The value to use for NoData if output_dtype is an integer type.
#                           Must be within the range of the chosen integer `output_dtype`.
#                           Default is 255.

#     Returns:
#     - xr.DataArray: A new DataArray with binary classification (1 for above threshold,
#                     0 for below threshold, and `nodata_value` for NoData areas).
#     """
#     mask = probability_da == 255
#     above_threshold = probability_da >= threshold

#     final_output = xr.where(above_threshold, 1, 0)
#     final_output = xr.where(mask, nodata_value, final_output).astype("uint8")

#     return final_output


# def extract_single_class(
#     classification: xr.DataArray, target_class_id: int, nodata_value: int = 255
# ) -> xr.DataArray:
#     one_class = classification == target_class_id
#     one_class = one_class.where(one_class == 1, 0)
#     one_class = one_class.where(~(classification == nodata_value), nodata_value).astype(
#         "uint8"
#     )

#     return one_class


def do_prediction(ds, model, target_class_id: int = 4):
    """Predicts the model on the dataset and adds the prediction as a new variable.

    Args:
        ds (Dataset): Dataset to predict on
        model (RegressorMixin): Model to predict with
        target_class_id (int): ID of the target class for prediction

    Returns:
        Dataset: Dataset with the prediction as a new variable
    """
    # Store the original mask
    mask = ds.red.isnull()  # Probably should check more bands

    # Convert to a stacked array of observations
    stacked_arrays = ds.to_array().stack(dims=["y", "x"])

    # Replace any infinities with NaN
    stacked_arrays = stacked_arrays.where(stacked_arrays != float("inf"))
    stacked_arrays = stacked_arrays.where(stacked_arrays != float("-inf"))

    # Replace any NaN values with 0 and transpose to the right shape
    stacked_arrays = stacked_arrays.squeeze().fillna(0).transpose().to_pandas()

    # Sort the columns by name
    stacked_arrays = stacked_arrays.reindex(sorted(stacked_arrays.columns), axis=1)

    # Remove the all-zero rows
    # This should make it MUCH MUCH faster, as we're not processing masked areas
    zero_mask = (stacked_arrays == 0).all(axis=1)
    non_zero_df = stacked_arrays.loc[~zero_mask]

    # Create a new array to hold the predictions
    full_predictions = pd.Series(np.nan, index=stacked_arrays.index)
    full_probabilities = pd.Series(np.nan, index=stacked_arrays.index)

    # Only run the prediction if there are non-zero rows
    if not non_zero_df.empty:
        # Predict the classes
        predictions = model.predict(non_zero_df)
        full_predictions.loc[~zero_mask] = predictions

        # Do the same for the probabilities
        probabilities = model.predict_proba(non_zero_df)
        target_class_index = list(model.classes_).index(target_class_id)
        target_probabilities = probabilities[:, target_class_index]
        target_probabilities_scaled = target_probabilities * 100
        full_probabilities.loc[~zero_mask] = target_probabilities_scaled

    # Reshape the results
    predicted = reshape_array_to_2d(full_predictions, ds, mask)
    probabilities = reshape_array_to_2d(full_probabilities, ds, mask)

    # Results should both be uint8 with 255 as nodata
    return predicted, probabilities


class LulcProcessor(Processor):
    def __init__(
        self,
        model,
        # probability_threshold: int = 60,
        nodata_value: int = 255,
        target_class_id: int = 4,
        # fast_mode: bool = True,
        logger: Logger | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._model = model
        # self._probability_threshold = probability_threshold
        self._nodata_value = nodata_value
        self._target_class_id = target_class_id
        # self._fast_mode = fast_mode

        if logger is None:
            self._logger = Logger("LULC_processor")
        else:
            self._logger = logger

    def process(self, input_data: xr.Dataset) -> xr.Dataset:
        self._logger.info("Starting processing of input data")
        # Scale data to values of 0-1 so that we can calculate indices properly
        scaled_data = scale_offset_landsat(input_data).squeeze(drop=True)

        # Load data into memory here
        self._logger.info("Loading data into memory...")
        # TODO: Should we delay loading?
        loaded_data = scaled_data.compute()

        # Compute indices
        self._logger.info("Computing band indices...")
        data = calculate_indices(loaded_data)

        # Run the prediction
        self._logger.info("Running prediction and probability process...")
        classification, probability = do_prediction(
            data, self._model, self._target_class_id
        )

        lulc_threshold = probability_binary(
            probability,
            self._probability_threshold,
            nodata_value=self._nodata_value,
        )

        lulc_class = extract_single_class(
            classification,
            self._target_class_id,
        )

        output = xr.Dataset(
            {
                "classification": classification,
                "lulc_probability": probability,
                "lulc_threshold_60": lulc_threshold,
                "lulc": lulc_class,
            }
        )

        for var in output.data_vars:
            output[var].odc.nodata = self._nodata_value
            output[var].attrs["_FillValue"] = self._nodata_value

        return output
    

def _load_joblib_model(model_path: str): # TODO: Type return to joblib model 
    if model_path.startswith("https://"):
        # Download the model.
        model_local = "classification/models/" + model_path.split("/")[-1]
        if not Path(model_local).exists():
            logger.info(f"Downloading model from {model_path} to {model_local}")
            r = requests.get(model_path)
            with open(model_local, "wb") as f:
                f.write(r.content)
        model = model_local

    elif model_path.endswith(".zip"):
        # Unzip.
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
        raise ValueError(f"Model path must be a '.joblib' file, a URL to a '.joblib' file, or a '.zip' file, not {model_path}")

    # Make sure we can open the model
    try:
        joblib_load(model)
        return model
    except Exception as e:
        logger.exception(f"Failed to load model from {model}: {e}")
        typer.Exit(code=1)


def run_predict_task(
    tile_id: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    region: Literal["pacific", "non-pacific"],
    output_bucket: str,
    # model_path: str = "classification/models/20250902c-alex.model", # Seagrass model
    model_path: str = "lulc_random_forest_model.joblib",
    # probability_threshold: int = 60,
    # fast_mode: bool = True,
    xy_chunk_size: int = 1024,
    asset_url_prefix: str | None = None,
    decimated: bool = False,
    overwrite: Annotated[bool, typer.Option()] = False,
) -> None:
    # logger = get_logger(tile_id, "LULC_prediction")
    logger.info("Starting processing")

    # Split by any of [",", "-", "_"] to be robust.
    tile_id_parts = [int(i) for i in re.split(r"[,\-_]", tile_id)]
    if len(tile_id_parts) != 2:
        raise ValueError(f"Tile ID must split into 2 integers, got {tile_id_parts} from tile_id '{tile_id}'")
    tile_id_tuple: tuple[int, int] = (tile_id_parts[0], tile_id_parts[1])

    # Get grid based on region.
    grid = get_gridspec(region)
    geobox = grid.tile_geobox(tile_id_tuple)

    if decimated:
        geobox = geobox.zoom_out(10)

    # Make sure we can access S3
    logger.info("Configuring S3 access")
    configure_s3_access(cloud_defaults=True)

    client = boto3.client("s3")

    loaded_model = _load_joblib_model(model_path)

    # # TODO: Why can output_bucket be None?
    # if output_bucket is None:
    #     logger.warning("Output bucket is None, skipping S3 existence check and STAC writing. ")

    itempath = S3ItemPath(
        bucket=output_bucket,
        sensor="landsat",
        dataset_id="lulc_prediction",
        version=version,
        time=datetime,
        full_path_prefix=asset_url_prefix,
    )
    stac_url = itempath.stac_path(tile_id)

    # If we don't want to overwrite, and the destination file already exists, skip it
    if not overwrite and object_exists(output_bucket, stac_url, client=client):
        logger.info(f"Item already exists at {itempath.stac_path(tile_id, absolute=True)}")
        raise typer.Exit() # Exit successfully.

    # searcher = PystacSearcher(
    #     # catalog="https://stac.digitalearthpacific.org",
    #     # collections=["dep_s2_geomad"],
    #     catalog=
    #     datetime=datetime,
    # )

    # loader = OdcLoader(
    #     chunks=dict(x=xy_chunk_size, y=xy_chunk_size),
    #     fail_on_error=False,
    #     measurements=[
    #         # TODO: Validate bands.
    #         "nir",
    #         "red",
    #         "blue",
    #         "green",
    #         "emad",
    #         "smad",
    #         "bcmad",
    #         "green",
    #         "nir08",
    #         "nir09",
    #         "swir16",
    #         "swir22",
    #     ],  # List measurements so we don't get count
    # )

    # The actual processor, doing the work
    processor = LulcProcessor(
        model=loaded_model,
        # probability_threshold=probability_threshold,
        nodata_value=255,
        # fast_mode=fast_mode,
        logger=logger,
    )

    stac_creator = StacCreator(itempath=itempath, with_raster=True)

    # TODO: Do the equivalent of get_geomad_dem_indices in a processor.
    # TODO: The DEP Seagrass setup has 1 of searcher, loader, processor, but get_geomad_dem_indices does 2 search and load, and 1 process. I think I should set up another processor.
    try:
        client = DaskClient(n_workers=4, threads_per_worker=16, memory_limit="12GB")
        logger.info(("Started dask client"))
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
        logger.info("No items found for this tile")
        raise typer.Exit()  # Exit successfully
    except Exception as e:
        logger.exception(f"Failed to process with error: {e}")
        raise typer.Exit(code=1)
    finally:
        client.close()

    logger.info(
        f"Completed processing. Wrote {len(paths)} items to {itempath.stac_path(tile_id, absolute=True)}"
    )
