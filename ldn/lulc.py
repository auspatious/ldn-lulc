import logging
from datetime import UTC
from datetime import datetime as dt
from functools import partial
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import requests
import typer
import xarray as xr
from dask.distributed import Client as DaskClient
from dep_tools.loaders import OdcLoader
from dep_tools.namers import GenericItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.utils import _fix_geometry
from geopandas import GeoDataFrame
from joblib import load as joblib_load
from odc.geo.geobox import GeoBox
from pystac import Item, ItemCollection, Link
from pystac.extensions.classification import ClassificationExtension
from rustac import search_sync
from sklearn.ensemble import RandomForestClassifier
from typing_extensions import Annotated

from ldn.aws import configure_s3_access_profile, s3_client
from ldn.grids import get_gridspec
from ldn.raster import (
    GEOMAD_BANDS,
    build_pipeline_components,
    make_features,
    stac_providers,
)
from ldn.raster import (
    AwsStacTask as Task,
)
from ldn.typology import stac_classes
from ldn.utils import (
    GEOMAD_DATASET_ID,
    GEOMAD_VERSION,
    LULC_DATASET_ID,
    LULC_VERSION,
    SOURCE_COOP_PREFIX_LULC,
    WGS84,
    LdnError,
    get_analysis_epsg,
    get_public_url_base,
    get_stac_geoparquet_key,
    get_stac_geoparquet_url,
    is_bucket_source_coop,
    owner_for_region,
    parse_tile_id,
)

logger = logging.getLogger(__name__)


class StacGeoparquetSearcher(Searcher):
    """Search a STAC-Geoparquet file for a single item by ID using rustac."""

    def __init__(self, stac_geoparquet_url: str, item_id: str):
        """Create a searcher for a STAC-Geoparquet file.

        Args:
            stac_geoparquet_url: HTTP(S) URL to the STAC-Geoparquet file.
            item_id: Full STAC item ID, e.g. "dep_ls_geomad_028_030_2000".
        """
        super().__init__()
        self._url = stac_geoparquet_url
        self._item_id = item_id

    def search(self, area: GeoBox | None = None) -> ItemCollection:
        """Search for the item with the configured ID.

        Args:
            area: Unused, because the item ID already identifies the tile. Accepted so this matches the dep_tools
                Searcher interface that AwsStacTask calls with the tile area.

        Returns:
            A pystac ItemCollection containing exactly one item.

        Raises:
            LdnError: If the number of matching items is not exactly one.
        """
        items = [Item.from_dict(doc) for doc in search_sync(self._url, ids=[self._item_id])]

        if len(items) != 1:
            raise LdnError(f"Expected exactly one GeoMAD item with ID {self._item_id}, but found {len(items)}")

        return ItemCollection(items)


def geomad_searcher(
    geomad_bucket: str,
    region: Literal["pacific", "non-pacific"],
    product_owner: str | None,
    single_region: bool,
    sensor: str,
    geomad_version: str,
    year: str,
    tile_id_tuple: tuple[int, int],
) -> StacGeoparquetSearcher:
    """Return a searcher for one GeoMAD tile and year, using the same item ID as the GeoMAD writer.

    Args:
        geomad_bucket: S3 bucket where the GeoMAD data is stored.
        region: Grid region, either "pacific" or "non-pacific".
        product_owner: Override the region-derived owner prefix.
        single_region: If True, search the owner's single region index (e.g. 'dep_ls_geomad'), otherwise the
            many-region index (e.g. 'ls_geomad').
        sensor: Sensor name (e.g. 'ls').
        geomad_version: GeoMAD version string (e.g. "0-3-0").
        year: Year string (e.g. "2020").
        tile_id_tuple: Grid tile index (x, y).

    Returns:
        A StacGeoparquetSearcher for the item.
    """
    owner = owner_for_region(region, product_owner)
    key = get_stac_geoparquet_key(
        geomad_bucket, owner if single_region else None, sensor, GEOMAD_DATASET_ID, geomad_version
    )
    # Same ID construction as the GeoMAD writer (itempath.basename), without its bucket region lookup.
    item_id = GenericItemPath(
        sensor=sensor,
        dataset_id=GEOMAD_DATASET_ID,
        version=geomad_version,
        time=year,
        prefix=owner,
    ).basename(tile_id_tuple)
    return StacGeoparquetSearcher(stac_geoparquet_url=get_stac_geoparquet_url(geomad_bucket, key), item_id=item_id)


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
        nodata_value: Integer nodata value for output pixels.

    Returns:
        A 2D uint8 DataArray with the specified nodata_value for nodata pixels.
    """
    array = stacked_array.to_numpy().reshape(template_ds.y.size, template_ds.x.size)
    da = xr.DataArray(array, coords={"y": template_ds.y, "x": template_ds.x}, dims=["y", "x"])
    # nodata_value as NoData. Ensure any remaining NaNs are also set to nodata_value before casting.
    da = da.where(~original_mask, nodata_value).fillna(nodata_value)
    return da.astype("uint8")


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
        probabilities is a dict mapping "probability_{class value}" (e.g. "probability_1" for Tree cover)
        to uint8 DataArrays of per-class probability (0-100).
    """
    stacked = ds.to_array().stack(dims=["y", "x"])

    # Nodata mask: True for pixels where ANY band is NaN.
    nodata_mask = stacked.isnull().any(dim="variable")

    # Build observation table: fill NaN with nodata_value (masked pixels are excluded below).
    obs = (
        stacked.squeeze()
        .fillna(nodata_value)
        .transpose()
        .to_dataset(dim="variable")  # pivot "variable" into named columns
        .to_dataframe()
    )

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

    # Per-class probability bands, named by class value (predict_proba columns follow model.classes_).
    probabilities = {}
    for i, class_value in enumerate(model.classes_):
        series = pd.Series(full_probabilities[:, i], index=obs.index)
        probabilities[f"probability_{int(class_value)}"] = reshape_array_to_2d(
            series, ds, nodata_mask_2d, nodata_value=nodata_value
        )

    # Classification = argmax class only where max probability >= threshold, else nodata.
    max_prob = np.stack([da.values for da in probabilities.values()], axis=0).max(axis=0)
    max_prob_da = xr.DataArray(max_prob, coords={"y": ds.y, "x": ds.x}, dims=["y", "x"])
    classification = predictions_2d.where(max_prob_da >= probability_threshold, nodata_value).astype("uint8")

    return classification, probabilities


class LulcProcessor(Processor):
    """Processor that scales GeoMAD, computes indices, loads terrain, and predicts classes."""

    def __init__(
        self,
        model: RandomForestClassifier,
        logger: logging.Logger,
        probability_threshold: float,
        nodata_value: int,
        year: str,
        **kwargs,
    ):
        """Create a LULC prediction/classification processor.

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
        """Build features (scaled GeoMAD, indices, DEM terrain) and predict/classify LULC.

        Args:
            input_data: GeoMAD dataset loaded by GeopolygonOdcLoader.

        Returns:
            Dataset with classification and probability bands.
        """
        self._logger.info("Building features: scaling GeoMAD, computing indices, loading DEM terrain")
        merged = make_features(input_data)

        # Compute before prediction: sklearn needs eager numpy arrays,
        # and sending a large lazy graph to Dask workers is slow.
        self._logger.info("Computing merged dataset")
        merged = merged.compute()

        self._logger.info("Running LULC prediction")
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
        The loaded local model file.

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
        return joblib_load(model)
    except Exception as e:
        raise LdnError(f"Failed to load model from {model}") from e


def _set_stac_properties(ds: xr.Dataset, year: str) -> xr.Dataset:
    """Set STAC temporal properties on the output LULC dataset."""

    ds.attrs["stac_properties"] = dict(
        datetime=f"{year}-06-30T00:00:00Z",
        created=dt.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    )

    return ds


def _add_lulc_stac_metadata(item: Item, model_path: str, nodata_value: int) -> None:
    """Link the model used and describe the classification classes on a LULC STAC item, in place.

    Args:
        item: LULC STAC item.
        model_path: Path or URL to the model used for classification.
        nodata_value: Integer nodata value of the classification band.
    """
    item.add_link(Link(rel="related", target=model_path, title="LULC classification model"))
    ClassificationExtension.ext(item.assets["classification"], add_if_missing=True).classes = stac_classes(nodata_value)


def run_classify_task(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    geomad_version: Annotated[str, typer.Option()],
    region: Literal["pacific", "non-pacific"],
    geomad_bucket: str,
    output_bucket: str,
    product_owner: str | None,
    single_region_geomad: bool,
    model_path: str,
    xy_chunk_size: int,
    decimated: bool,
    integration_test: bool,
    overwrite: Annotated[bool, typer.Option()],
    probability_threshold: float,
    nodata_value: int,
    memory_limit: str,
    n_workers: int,
    threads_per_worker: int,
    sensor: str,
    collection_url_root: str | None,
) -> None:
    """Run LULC prediction for a single tile and year, writing results to S3.

    Uses GeopolygonOdcLoader (geopolygon= with AM-fixing instead of
    geobox=) so antimeridian-crossing tiles load correctly.

    Args:
        tile_id: Grid tile identifier (e.g. "136_142").
        year: Year string (e.g. "2020").
        version: Output version string (e.g. "0-0-1").
        geomad_version: Version of the GeoMAD data to use (e.g. "0-0-1").
        region: Grid region, either "pacific" or "non-pacific".
        geomad_bucket: S3 bucket for input GeoMAD source data.
        output_bucket: S3 bucket for output COGs and STAC metadata.
        product_owner: Override the region-derived owner prefix.
        single_region_geomad: If True, search the owner's single region GeoMAD index, otherwise the many-region index.
        model_path: Path or URL to the trained joblib model.
        xy_chunk_size: Chunk size in pixels for lazy loading.
        decimated: If True, use 10x lower resolution (for testing).
        integration_test: If True, use subset of data for faster processing in integration tests.
        overwrite: If True, overwrite existing output.
        probability_threshold: Confidence threshold (0-100) for the binary mask.
        nodata_value: Integer nodata value for output bands.
        memory_limit: Per-worker Dask memory limit.
        n_workers: Number of Dask workers.
        threads_per_worker: Number of threads per Dask worker.
        sensor: Sensor to use for LULC classification e.g. 'ls'.
        collection_url_root: Override the default collection URL root for STAC metadata.
    """
    logger.info(f"Starting processing. Tile ID: {tile_id}, Year: {year}, Region: {region}, Version: {version}.")
    logger.info(
        f"Dask config: n_workers={n_workers}, threads_per_worker={threads_per_worker}, "
        f"memory_limit={memory_limit}, xy_chunk_size={xy_chunk_size}"
    )

    if geomad_version != GEOMAD_VERSION:
        logger.info(
            f"Overriding the latest GeoMAD version ({GEOMAD_VERSION}) with the specified version ({geomad_version})."
        )
    if version != LULC_VERSION:
        logger.info(
            f"Overriding the latest LULC prediction version ({LULC_VERSION}) with the specified version ({version})."
        )

    tile_id_tuple = parse_tile_id(tile_id)

    analysis_crs = get_analysis_epsg(region)

    logger.info("Getting gridspec and geobox for tile")
    grid = get_gridspec(region)
    geobox = grid.tile_geobox(tile_id_tuple)

    if decimated:
        logger.warning("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    if integration_test:
        logger.warning("Integration test mode: using 5x5 pixel geobox for very fast processing.")
        geobox = geobox[0:5, 0:5]
        n_workers = 1
        threads_per_worker = 1
        memory_limit = "1GB"

    configure_s3_access_profile()  # Access must be configured here for Dask.

    logger.info("Loading model")
    loaded_model = _load_joblib_model(model_path)

    # TODO: Could this block be a function because it will be done in cli_collection.py?
    _is_source_coop = is_bucket_source_coop(output_bucket)
    public_url = get_public_url_base(output_bucket)
    if _is_source_coop:
        public_url = f"{public_url}/{SOURCE_COOP_PREFIX_LULC}"
    collection_url_root = collection_url_root or f"{public_url}/collections"

    owner = owner_for_region(region, product_owner)

    components = build_pipeline_components(
        tile_id_tuple,
        year,
        version,
        output_bucket,
        owner,  # LULC output is always written under an owner prefix (e.g. 'dep_ls_lulc').
        LULC_DATASET_ID,
        SOURCE_COOP_PREFIX_LULC if _is_source_coop else None,
        overwrite,
        collection_url_root=collection_url_root,
        s3_client=s3_client,
        sensor=sensor,
    )
    if components is None:
        return  # Skip due to no overwrite.
    itempath, stac_creator, writer = components

    searcher = geomad_searcher(
        geomad_bucket,
        region,
        product_owner,
        single_region_geomad,
        sensor,
        geomad_version,
        year,
        tile_id_tuple,
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
        model=loaded_model,
        nodata_value=nodata_value,
        logger=logger,
        probability_threshold=probability_threshold,
        year=year,
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
                providers=stac_providers(LULC_DATASET_ID, _is_source_coop),
                processing_software=[
                    "ldn",
                    "dep-tools",
                    "odc-stac",
                    "odc-geo",
                    "rasterio",
                    "rioxarray",
                    "gdal",
                    "numpy",
                    "scipy",
                    "xarray",
                    "scikit-learn",
                ],
                update_item=partial(_add_lulc_stac_metadata, model_path=model_path, nodata_value=nodata_value),
                logger=logger,
                writer=writer,
                stac_creator=stac_creator,
            ).run()
            logger.info(
                f"Completed processing. Wrote {len(paths)} files to {itempath.stac_path(tile_id_tuple, absolute=True)}"
            )

    except Exception:
        logger.exception("Failed to process with error")
        raise  # let it exit 1 naturally with full traceback
