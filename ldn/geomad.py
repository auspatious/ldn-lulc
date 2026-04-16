from datetime import datetime
import logging
from typing import Iterable, Tuple

from datacube_compute import geomedian_with_mads
from dep_tools.loaders import StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AreaTask
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from odc.geo import GeoBox
import numpy as np
from odc.algo import mask_cleanup
from xarray import Dataset
from ldn.utils import LdnError

logger = logging.getLogger(__name__)

USGS_CATALOG = "https://earth-search.aws.element84.com/v1"
USGS_COLLECTION = "landsat-c2-l2"

LANDSAT_BANDS = [
    "qa_pixel",
    "qa_radsat",
    "red",
    "green",
    "blue",
    "nir08",
    "swir16",
    "swir22",
]
LANDSAT_SCALE = 0.0000275
LANDSAT_OFFSET = -0.2


def _to_utc_ms_string(dt: np.datetime64) -> str:
    return str(np.datetime_as_string(dt, unit="ms", timezone="UTC"))


def http_to_s3_url(http_url):
    """Convert a USGS HTTP URL to an S3 URL"""
    s3_url = http_url.replace(
        "https://landsatlook.usgs.gov/data", "s3://usgs-landsat"
    ).rstrip(":1")
    return s3_url


def set_stac_properties(input_xr: Dataset, output_xr: Dataset) -> Dataset:
    start_year = np.datetime64(input_xr.time.min().values, "Y")
    end_year = np.datetime64(input_xr.time.max().values, "Y")
    start_year_index = int(start_year.astype("int64"))
    end_year_index = int(end_year.astype("int64"))

    start_datetime = _to_utc_ms_string(start_year)
    end_datetime = _to_utc_ms_string(
        end_year + np.timedelta64(1, "Y") - np.timedelta64(1, "s")
    )

    midpoint_year_index = (start_year_index + end_year_index) // 2
    midpoint_year = 1970 + midpoint_year_index
    midpoint_datetime = f"{midpoint_year}-06-30T00:00:00.000Z"

    output_xr.attrs["stac_properties"] = dict(
        start_datetime=start_datetime,
        datetime=midpoint_datetime,
        end_datetime=end_datetime,
        created=_to_utc_ms_string(np.datetime64(datetime.now())),
    )

    return output_xr


# TODO: In version 0-0-5, need to test this because it's been refactored to preserve QA bands and only apply masking to spectral bands.
def mask_nodata(ds: Dataset, nodata_value: int = 0) -> Dataset:
    """Mask nodata and fill pixels, preserving QA bands.

    Applies masking only to spectral bands so that qa_pixel and
    qa_radsat retain their original values for downstream use.

    Args:
        ds: Input dataset with Landsat bands.
        nodata_value: Value used to identify and fill nodata pixels.

    Returns:
        Dataset with spectral bands masked, QA bands unchanged.
    """
    qa_bands = {"qa_pixel", "qa_radsat"}
    spectral_bands = [b for b in ds.data_vars if b not in qa_bands]

    # Combine nodata from all spectral bands into a single mask.
    nodata_mask = np.zeros_like(ds[spectral_bands[0]], dtype=bool)
    for band in spectral_bands:
        nodata_mask = nodata_mask | (ds[band] == nodata_value)

    if "qa_pixel" in ds.data_vars:
        FILL = 0
        fill_mask = (ds["qa_pixel"].astype(int) & (1 << FILL)) != 0
        nodata_mask = nodata_mask | fill_mask

    for sband in spectral_bands:
        # Must use other here so uint16 values don't get converted to float32 with nan.
        ds[sband] = ds[sband].where(~nodata_mask, other=nodata_value)

    return ds


def mask_cloud_and_shadow(
    ds: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    include_shadow: bool = True,
    nodata_value: int = 0,
) -> Dataset:
    """
    Mask out cloud, cirrus, and optionally shadow pixels using qa_pixel bits.
    Args:
        ds: Input xarray Dataset.
        filters: Morphological filter sequence applied to the cloud mask only.
        include_shadow: Whether to include cloud shadow (qa_pixel bit 4).
    Returns:
        Masked xarray Dataset.
    """
    DILATED_CLOUD = 1
    CIRRUS = 2
    CLOUD = 3
    CLOUD_SHADOW = 4

    cloud_fields = [DILATED_CLOUD, CIRRUS, CLOUD]
    if include_shadow:
        cloud_fields.append(CLOUD_SHADOW)

    cloud_bitmask = 0
    for field in cloud_fields:
        cloud_bitmask |= 1 << field

    qa_pixel = ds["qa_pixel"] if "qa_pixel" in ds.data_vars else ds.qa_pixel
    cloud_mask = (qa_pixel.astype(int) & cloud_bitmask) != 0

    if filters is not None:
        # Add morphological filters to cloud/shadow mask only.
        cloud_mask = mask_cleanup(cloud_mask, filters)

    # Add a mask for medium confidence clouds. Don't dilate them though.
    CLOUD_CONFIDENCE_SHIFT = 8
    CLOUD_CONFIDENCE_MEDIUM = 2
    # CLOUD_CONFIDENCE_HIGH = 3
    # TODO: replace 0b11 with a named constant for clarity.
    cloud_confidence = (qa_pixel.astype(int) >> CLOUD_CONFIDENCE_SHIFT) & 0b11
    cloud_confidence_mask = cloud_confidence >= CLOUD_CONFIDENCE_MEDIUM

    cloud_confidence_mask = mask_cleanup(cloud_confidence_mask, [("opening", 2)])

    # Must use other here so uint16 values don't get converted to float32 with nan.
    return ds.where(~(cloud_mask | cloud_confidence_mask), other=nodata_value)


def mask_saturated(ds: Dataset, nodata_value: int = 0) -> Dataset:
    if "qa_radsat" in ds.data_vars:
        # Must use other here so uint16 values don't get converted to float32 with nan.
        ds = ds.where(ds.qa_radsat == 0, other=nodata_value)

    for band in ["red", "green", "blue"]:
        if band in ds.data_vars:
            # Must use other here so uint16 values don't get converted to float32 with nan.
            # ds = ds.where(ds[band] != 65_535, other=nodata_value)
            # This catches overly saturated pixels (after qa_pixel and qa_radsat masking).
            ds = ds.where(ds[band] < 43_636, other=nodata_value)

    return ds


def mask_nodata_clouds_saturated(
    ds: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    include_shadow: bool = True,
) -> Dataset:
    # Only valid for LS8 and LS9, but we can still apply
    # it to LS7 data without error, it just won't mask anything.
    """Mask clouds, shadows, fill, and saturated pixels from Landsat data.

    Morphological filters (opening, dilation, etc.) are applied only to the
    cloud/shadow mask so that they do not widen non-cloud artefacts such as
    Landsat 7 SLC-off gaps or sensor saturation holes.

    Args:
        ds: Input dataset containing qa_pixel and optionally qa_radsat.
        filters: Morphological filter sequence applied to the cloud mask only.
        include_shadow: Whether to include cloud shadow (qa_pixel bit 4).
    """
    ds = mask_nodata(ds)

    ds = mask_cloud_and_shadow(ds, filters=filters, include_shadow=include_shadow)

    ds = mask_saturated(ds)

    # return erase_bad(ds, combined_mask)
    # Performance seems fine using this method (compared to erase_bad), but could be checked more closely.
    return ds


class GeoMADProcessor(Processor):
    def __init__(
        self,
        send_area_to_processor: bool = False,
        load_data_before_writing: bool = True,
        min_timesteps: int = 10,
        geomad_options: dict = {
            "num_threads": 4,
            "work_chunks": (1000, 1000),
            "maxiters": 1000,
        },
        drop_vars: list[str] = [],
        preprocessor: Processor | None = None,
        mask_clouds_kwargs: dict = {
            "filters": [("opening", 3), ("dilation", 5), ("erosion", 2)],
            "include_shadow": True,
        },
        **kwargs,
    ) -> None:
        super().__init__(send_area_to_processor, **kwargs)
        self.load_data_before_writing = load_data_before_writing
        self.min_timesteps = min_timesteps
        self.geomad_options = geomad_options
        self.drop_vars = drop_vars
        self.preprocessor = preprocessor
        self.mask_kwargs = mask_clouds_kwargs

    def process(self, ds: Dataset) -> Dataset:
        if ds.time.size < self.min_timesteps:
            raise LdnError(
                f"{ds.time.size} is less than {self.min_timesteps} timesteps"
            )

        ds = mask_nodata_clouds_saturated(ds, **self.mask_kwargs)
        data = ds.drop_vars(self.drop_vars) if len(self.drop_vars) > 0 else ds

        geomad = geomedian_with_mads(data, **self.geomad_options)

        if self.load_data_before_writing:
            geomad = geomad.compute()

        geomad[
            "count"
        ].odc.nodata = (
            0  # This could hide real values of 0. 9999 is what datacube-compute do.
        )

        return set_stac_properties(data, geomad)


class AwsStacTask(AreaTask):
    """Area task with search + STAC creation/writing for AWS workflows."""

    def __init__(
        self,
        itempath: S3ItemPath,
        id: str,  # TODO: Check this type. str or tuple?
        area: GeoBox,
        searcher: Searcher,
        loader: StacLoader,
        processor: Processor,
        post_processor: Processor | None = None,
        logger: logging.Logger = logger,
        **kwargs,
    ):
        writer = kwargs.pop("writer", AwsDsCogWriter(itempath))
        stac_creator = kwargs.pop("stac_creator", StacCreator(itempath))
        stac_writer = kwargs.pop("stac_writer", AwsStacWriter(itempath))

        super().__init__(id, area, loader, processor, writer, logger)
        self.id = id
        self.searcher = searcher
        self.post_processor = post_processor
        self.stac_creator = stac_creator
        self.stac_writer = stac_writer

    def run(self):
        items = self.searcher.search(self.area)
        logger.info(f"Found {len(items)} LS items for this tile/year")
        input_data = self.loader.load(items, self.area)
        logger.info(
            f"Loaded {len(input_data.time.values)} LS items for this tile/year (grouped by solar_day)"
        )

        processor_kwargs = (
            dict(area=self.area) if self.processor.send_area_to_processor else dict()
        )
        output_data = self.processor.process(input_data, **processor_kwargs)

        if self.post_processor is not None:
            output_data = self.post_processor.process(output_data)

        paths = self.writer.write(output_data, self.id)

        if self.stac_creator is not None and self.stac_writer is not None:
            stac_item = self.stac_creator.process(output_data, self.id)
            self.stac_writer.write(stac_item, self.id)

        return paths
