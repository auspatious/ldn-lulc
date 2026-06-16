import logging
from datetime import UTC
from datetime import datetime as dt
from pathlib import Path
from typing import Literal

import boto3
import numpy as np
import pandas as pd
import requests
import rioxarray  # noqa: F401 for the .rio accessor
import typer
import xarray as xr
from dask.distributed import Client as DaskClient
from dep_tools.aws import object_exists
from dep_tools.loaders import OdcLoader
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.utils import _fix_geometry
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from geopandas import GeoDataFrame
from joblib import load as joblib_load
from odc.geo.geobox import GeoBox
from odc.stac import configure_s3_access
from pystac import Item, ItemCollection
from rustac import search_sync
from sklearn.ensemble import RandomForestClassifier
from typing_extensions import Annotated

from ldn.aws_credentials import get_write_session, make_write_function
from ldn.geomad import AwsStacTask as Task
from ldn.grids import get_gridspec
from ldn.raster import (
    GEOMAD_BANDS,
    PrefixedS3ItemPath,
    calculate_indices,
    load_dem_terrain,
    scale_offset_landsat,
)
from ldn.utils import (
    GEOMAD_VERSION,
    PREDICTION_DATASET_ID,
    PREDICTION_VERSION,
    SENSOR,
    SOURCE_COOP_PREFIX_PREDICTION,
    SOURCE_COOP_PUBLIC_URL,
    WGS84,
    LdnError,
    get_analysis_epsg,
    get_collection_url_root,
    get_full_path_prefix,
    get_geomad_stac_geoparquet_url,
    parse_tile_id,
)

logger = logging.getLogger(__name__)


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

        # TODO: Should len(items) == 1??

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
            tile_gdf = GeoDataFrame(geometry=[tile_geom], crs=areas.crs).to_crs(WGS84)
            fixed = _fix_geometry(tile_gdf.geometry.iloc[0])
            areas = GeoDataFrame(geometry=[fixed], crs=WGS84)

        result = super().load(items, areas)

        # Crop to the original geobox extent. geopolygon-based loading
        # may return a larger extent when the WGS84 footprint overlaps
        # neighbouring STAC items at tile boundaries.
        if original_geobox is not None and result.odc.geobox != original_geobox:
            logger.info(f"Cropping loaded data from {result.odc.geobox.shape} to target geobox {original_geobox.shape}")
            result = result.odc.crop(original_geobox.extent, apply_mask=False)

        return result


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
    da = xr.DataArray(array, coords={"y": template_ds.y, "x": template_ds.x}, dims=["y", "x"])
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
) -> tuple[xr.DataArray, dict[str, xr.DataArray]]:
    """Run random forest prediction and extract per-class probabilities.

    Converts the dataset to a flat observation table, runs the model,
    and reshapes results back to 2D.

    Args:
        ds: Feature dataset with y/x spatial dimensions.
        model: Fitted scikit-learn classifier with predict/predict_proba.
        probability_threshold: Confidence threshold (0-100) below which
            classification is set to nodata.
        nodata_value: Integer nodata value for output bands.

    Returns:
        A (classification, probabilities) tuple where classification is the
        argmax class where max probability >= threshold (else nodata), and
        probabilities is a dict mapping "probability_1" through "probability_N"
        to uint8 DataArrays of per-class probability (0-100).
    """
    stacked = ds.to_array().stack(dims=["y", "x"])

    # Nodata mask: True for pixels where ANY band is NaN.
    nodata_mask = stacked.isnull().any(dim="variable")

    # Build observation table: fill NaN with nodata_value (masked pixels are excluded below).
    obs = stacked.squeeze().fillna(nodata_value).transpose().to_dataframe()

    # Validate that all model features are present before reindexing.
    missing = set(model.feature_names_in_) - set(obs.columns)
    if missing:
        raise LdnError(f"Dataset is missing features required by the model: {sorted(missing)}")
    obs = obs.reindex(columns=model.feature_names_in_)

    # Flatten the spatial nodata mask to match the observation index.
    valid = ~nodata_mask.values

    n_classes = len(model.classes_)
    full_predictions = pd.Series(nodata_value, index=obs.index, dtype=np.float32)
    full_probabilities = np.full((len(obs.index), n_classes), nodata_value, dtype=np.float32)

    if valid.any():
        valid_df = obs.loc[valid]
        proba = model.predict_proba(valid_df)
        full_predictions.loc[valid] = model.classes_[proba.argmax(axis=1)].astype(np.float32)
        full_probabilities[valid] = (proba * 100).astype(np.float32)

    # Reshape back to 2D; nodata_mask stamps nodata_value over masked pixels.
    nodata_mask_2d = nodata_mask.unstack("dims")
    predictions_2d = reshape_array_to_2d(full_predictions, ds, nodata_mask_2d, nodata_value=nodata_value)

    # Per-class probability bands.
    probabilities = {}
    for i in range(n_classes):
        series = pd.Series(full_probabilities[:, i], index=obs.index)
        probabilities[f"probability_{i + 1}"] = reshape_array_to_2d(
            series, ds, nodata_mask_2d, nodata_value=nodata_value
        )

    # Classification = argmax class only where max probability >= threshold, else nodata.
    max_prob = np.stack([da.values for da in probabilities.values()], axis=0).max(axis=0)
    max_prob_da = xr.DataArray(max_prob, coords={"y": ds.y, "x": ds.x}, dims=["y", "x"])
    classification = predictions_2d.where(max_prob_da >= probability_threshold, nodata_value).astype("uint8")

    return classification, probabilities


class LulcProcessor(Processor):
    """Processor that scales GeoMAD, computes indices, loads terrain, and predicts."""

    def __init__(
        self,
        model: RandomForestClassifier,
        logger: logging.Logger,
        probability_threshold: float,
        nodata_value: int,
        year: str,
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
        self._year = year

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

        # Merge GeoMAD features with terrain features (override join to prefer GeoMAD coords and metadata).
        merged = xr.merge([data, dem_ds], join="override")

        # Compute before prediction: sklearn needs eager numpy arrays,
        # and sending a large lazy graph to Dask workers is slow.
        self._logger.info("Computing merged dataset")
        merged = merged.compute()

        self._logger.info("Running prediction")
        classification, probabilities = do_prediction(
            merged, self._model, self._probability_threshold, self._nodata_value
        )

        output = xr.Dataset(
            {
                "classification": classification,
                **probabilities,
            }
        )

        for var in output.data_vars:
            output[var].odc.nodata = self._nodata_value
            output[var].attrs["_FillValue"] = self._nodata_value

        return _set_stac_properties(output, year=self._year)


# TODO: Handle different bucket styles in _load_joblib_model
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
        raise LdnError(f"Model path must be a '.joblib' file or a URL to a '.joblib' file, not {model_path}")

    try:
        joblib_load(model)
        return model
    except Exception as e:
        raise LdnError(f"Failed to load model from {model}") from e


def _set_stac_properties(ds: xr.Dataset, year: str) -> xr.Dataset:
    """Set STAC temporal properties on the output LULC dataset."""

    ds.attrs["stac_properties"] = dict(
        datetime=f"{year}-06-30T00:00:00Z",
        created=dt.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    )

    return ds


def run_classify_task(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    version_geomad: Annotated[str, typer.Option()],
    region: Literal["pacific", "non-pacific"],
    bucket: str,
    owner: str,
    model_path: str,
    xy_chunk_size: int,
    decimated: bool,
    overwrite: Annotated[bool, typer.Option()],
    probability_threshold: float,
    nodata_value: int,
    memory_limit: str,
    n_workers: int,
    threads_per_worker: int,
) -> None:
    """Run LULC prediction for a single tile and year, writing results to S3.

    Uses GeopolygonOdcLoader (geopolygon= with AM-fixing instead of
    geobox=) so antimeridian-crossing tiles load correctly.

    Args:
        tile_id: Grid tile identifier (e.g. "136_142").
        year: Year string (e.g. "2020").
        version: Output version string (e.g. "0-0-1").
        version_geomad: Version of the GeoMAD data to use (e.g. "0-0-1").
        region: Grid region, either "pacific" or "non-pacific".
        bucket: S3 bucket for output COGs, STAC metadata, and input GeoMAD source data.
        owner: Output prefix for paths (e.g. "dep" or "ci" or owner override).
        model_path: Path or URL to the trained joblib model.
        xy_chunk_size: Chunk size in pixels for lazy loading.
        decimated: If True, use 10x lower resolution (for testing).
        overwrite: If True, overwrite existing output.
        probability_threshold: Confidence threshold (0-100) for the binary mask.
        nodata_value: Integer nodata value for output bands.
        memory_limit: Per-worker Dask memory limit.
        n_workers: Number of Dask workers.
        threads_per_worker: Number of threads per Dask worker.
    """
    logger.info(f"Starting processing. Tile ID: {tile_id}, Year: {year}, Region: {region}, Version: {version}.")

    if version_geomad != GEOMAD_VERSION:
        logger.info(
            f"Overriding the latest GeoMAD version ({GEOMAD_VERSION}) with the specified version ({version_geomad})."
        )
    if version != PREDICTION_VERSION:
        logger.info(
            f"Overriding the latest prediction version ({PREDICTION_VERSION}) with the specified version ({version})."
        )

    geomad_stac_geoparquet_url = get_geomad_stac_geoparquet_url(bucket=bucket, version=version_geomad)

    tile_id_tuple = parse_tile_id(tile_id)

    analysis_crs = get_analysis_epsg(region)

    logger.info("Getting gridspec and geobox for tile")
    grid = get_gridspec(region)
    geobox = grid.tile_geobox(tile_id_tuple)

    if decimated:
        logger.warning("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    logger.info("Configuring S3 access")
    configure_s3_access(cloud_defaults=True)

    logger.info("Loading model")
    loaded_model = _load_joblib_model(model_path)

    is_public = bool(SOURCE_COOP_PUBLIC_URL)  # Source.Coop.

    full_path_prefix = get_full_path_prefix(bucket)
    logger.info(f"Full path prefix: {full_path_prefix}")

    itempath = PrefixedS3ItemPath(
        key_prefix=SOURCE_COOP_PREFIX_PREDICTION if is_public else None,
        prefix=owner,
        bucket=bucket,
        sensor=SENSOR,
        dataset_id=PREDICTION_DATASET_ID,
        version=version,
        time=year,
        full_path_prefix=full_path_prefix,
    )
    stac_document = itempath.stac_path(tile_id_tuple, absolute=True)
    stac_key = itempath.stac_path(tile_id_tuple, absolute=False)

    write_session = get_write_session()
    write_client = write_session.client("s3")

    aws_client_to_use = write_client if is_public else boto3.client("s3")

    if not overwrite and object_exists(bucket, stac_key, client=aws_client_to_use):
        logger.info(f"Item already exists at {stac_document}, skipping.")
        return
    logger.info("Either item does not exist or overwrite is True, proceeding with processing.")

    logger.info(
        f"Dask config: n_workers={n_workers}, threads_per_worker={threads_per_worker}, "
        f"memory_limit={memory_limit}, xy_chunk_size={xy_chunk_size}"
    )

    searcher = StacGeoparquetSearcher(
        stac_geoparquet_url=geomad_stac_geoparquet_url,
        datetime=year,
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
        year=year,
    )

    stac_creator = StacCreator(
        collection_url_root=get_collection_url_root(bucket, owner, SENSOR, PREDICTION_DATASET_ID),
        itempath=itempath,
        with_raster=True,
    )

    writer = AwsDsCogWriter(
        itempath,
        write_multithreaded=True,
        write_function=make_write_function(write_session),
    )

    stac_writer = AwsStacWriter(
        itempath,
        client=aws_client_to_use,
    )

    try:
        with DaskClient(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
        ):
            paths = Task(
                itempath=itempath,
                id=tile_id_tuple,
                area=geobox,
                searcher=searcher,
                loader=loader,
                processor=processor,
                logger=logger,
                writer=writer,
                stac_creator=stac_creator,
                stac_writer=stac_writer,
            ).run()
            typer.echo(f"Completed processing. Wrote {len(paths)} files to {stac_document}")

    except Exception as e:
        logger.exception(f"Failed to process with error: {e}")
        raise  # let it exit 1 naturally with full traceback
