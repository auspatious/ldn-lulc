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
from xarray import Dataset, DataArray
from ldn.utils import LS7_YEAR_THRESHOLD, LdnError

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

qa_bands = {"qa_pixel", "qa_radsat"}


def _to_utc_ms_string(dt: np.datetime64) -> str:
    return str(np.datetime_as_string(dt, unit="ms", timezone="UTC"))


def http_to_s3_url(http_url):
    """Convert a USGS HTTP URL to an S3 URL"""
    s3_url = http_url.replace(
        "https://landsatlook.usgs.gov/data", "s3://usgs-landsat"
    ).rstrip(":1")
    return s3_url


def set_stac_properties(input_xr: Dataset, output_xr: Dataset) -> Dataset:
    f"""Set STAC temporal properties on the output dataset.

    The datetime fields represent the nominal target year (the year the
    GeoMAD product represents), not the full observation window. For LS7-era
    products (<={LS7_YEAR_THRESHOLD}) that use a multi-year buffer, the actual observation window is
    stored in custom properties for provenance.
    """
    start_year = np.datetime64(input_xr.time.min().values, "Y")
    end_year = np.datetime64(input_xr.time.max().values, "Y")
    start_year_index = int(start_year.astype("int64"))
    end_year_index = int(end_year.astype("int64"))

    midpoint_year_index = (start_year_index + end_year_index) // 2
    midpoint_year = 1970 + midpoint_year_index

    # Nominal year boundaries for STAC temporal search.
    start_datetime = f"{midpoint_year}-01-01T00:00:00Z"
    end_datetime = f"{midpoint_year}-12-31T23:59:59Z"
    midpoint_datetime = f"{midpoint_year}-06-30T00:00:00Z"

    properties = dict(
        start_datetime=start_datetime,
        datetime=midpoint_datetime,
        end_datetime=end_datetime,
        created=_to_utc_ms_string(np.datetime64(datetime.now())),
    )

    # Record the actual observation window when it differs from the nominal year.
    obs_start_year = 1970 + start_year_index
    obs_end_year = 1970 + end_year_index
    if obs_start_year != midpoint_year or obs_end_year != midpoint_year:
        properties["ldn:observation_start"] = f"{obs_start_year}-01-01T00:00:00Z"
        properties["ldn:observation_end"] = f"{obs_end_year}-12-31T23:59:59Z"

    output_xr.attrs["stac_properties"] = properties

    return output_xr


def fuse_qa_pixel(dst, src):
    """Fuse qa_pixel by copying src where dst is fill (0 or 1).

    dst = earlier image on the same solarday
    src = later image on the same solarday.
    This is used to preserve qa_pixel values for pixels that are nodata in earlier images but not later images on the same solarday.

    qa_pixel uses bit 0 as Fill, so fill=1 in metadata. But actual
    nodata pixels can also be 0 (no bits set). This function treats
    both 0 and 1 as empty and overwrites them with src.
    """
    np.copyto(dst, src, where=((dst == 0) | (dst == 1)))


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
        # Must use "other=" here so uint16 values don't get converted to float32 with nan.
        ds[sband] = ds[sband].where(~nodata_mask, other=nodata_value)

    return ds


# TODO: Look into sr_qa_aerosol band for masking haze.
def mask_qa_pixel(
    ds: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    mask_shadow: bool = True,
    mask_snow: bool = True,
    nodata_value: int = 0,
) -> Dataset:
    """Mask out cloud, cirrus, and optionally shadow/snow pixels using qa_pixel bits.

    Only masks spectral bands, preserving qa_pixel and qa_radsat.

    Keep logic: Clear bit set AND cloud/cirrus confidence fields are
    low or none.

    Mask logic: any of Dilated Cloud, Cirrus, Cloud bits set, or
    pixel not in keep set. Morphological filters are applied only to
    this cloud/cirrus mask.

    Shadow and snow are added after morphological filtering so they
    are not widened. They use both bit flags and confidence fields
    (medium or high confidence triggers masking).

    Args:
        ds: Input xarray Dataset.
        filters: Morphological filter sequence applied to the cloud mask only.
        mask_shadow: Whether to mask cloud shadow (bit 4 or confidence >= 2).
        mask_snow: Whether to mask snow pixels (bit 5 or confidence >= 2).
        nodata_value: Value to fill masked pixels with.

    Returns:
        Masked xarray Dataset with QA bands preserved.
    """
    DILATED_CLOUD = 1
    CIRRUS = 2
    CLOUD = 3
    CLOUD_SHADOW = 4
    SNOW = 5
    CLEAR = 6
    WATER = 7

    qa_pixel = ds["qa_pixel"]
    valid = (qa_pixel != 0) & (qa_pixel != 1)

    # Confidence fields (2-bit each): 0=None, 1=Low, 2=Medium, 3=High
    cloud_confidence = (qa_pixel >> 8) & 3
    shadow_confidence = (qa_pixel >> 10) & 3
    snow_confidence = (qa_pixel >> 12) & 3
    cirrus_confidence = (qa_pixel >> 14) & 3

    # Keep logic uses only cloud/cirrus confidence for the cloud mask
    # that gets morphologically filtered. Shadow/snow confidence is
    # applied separately after filtering so they are not widened.
    is_clear = (qa_pixel & (1 << CLEAR)) != 0
    cloud_cirrus_conf_low = (cloud_confidence <= 1) & (cirrus_confidence <= 1)
    is_water = (qa_pixel & (1 << WATER)) != 0
    is_snow = (qa_pixel & (1 << SNOW)) != 0
    if mask_snow:
        keep = (is_clear | is_water) & cloud_cirrus_conf_low
    else:
        keep = (is_clear | is_water | is_snow) & cloud_cirrus_conf_low

    # Explicitly mask bad bits regardless of Clear.
    bad_bits = (1 << DILATED_CLOUD) | (1 << CIRRUS) | (1 << CLOUD)
    has_bad_bit = (qa_pixel & bad_bits) != 0

    # Cloud mask for morphological filtering (only cloud/cirrus bits).
    cloud_mask = valid & (has_bad_bit | ~keep)

    if filters is not None:
        cloud_mask = mask_cleanup(cloud_mask, filters)

    # Add shadow and snow after morphological filters so they are not widened.
    if mask_shadow:
        shadow_mask = ((qa_pixel & (1 << CLOUD_SHADOW)) != 0) | (shadow_confidence >= 2)
        cloud_mask = cloud_mask | (valid & shadow_mask)

    if mask_snow:
        snow_mask = ((qa_pixel & (1 << SNOW)) != 0) | (snow_confidence >= 2)
        cloud_mask = cloud_mask | (valid & snow_mask)

    spectral_bands = [b for b in ds.data_vars if b not in qa_bands]
    for band in spectral_bands:
        ds[band] = ds[band].where(~cloud_mask, other=nodata_value)

    return ds


def mask_saturated(ds: Dataset, nodata_value: int = 0) -> Dataset:
    """Mask saturated pixels, preserving QA bands.

    Args:
        ds: Input dataset with qa_radsat band.
        nodata_value: Value to fill masked pixels with.

    Returns:
        Dataset with spectral bands masked where saturated, QA bands unchanged.
    """
    if "qa_radsat" in ds.data_vars:
        # In qa_radsat, 0 means: no bits are set, so no bands are saturated.
        # So mask any non-0 values.
        saturated_mask = ds["qa_radsat"] != 0
        spectral_bands = [b for b in ds.data_vars if b not in qa_bands]
        for band in spectral_bands:
            ds[band] = ds[band].where(~saturated_mask, other=nodata_value)

    return ds


# Custom mask cloud function that uses whiteness, blueness etc.
# This masks pixels that are clearly snow, but are not labelled as cloud in qa_pixel.

# Reflectance thresholds for hard cloud detection
_CLOUD_BLUE_MIN = 0.35  # beaches rarely exceed this
_CLOUD_WHITENESS_MAX = 0.15  # beaches are warmer toned, not truly white/grey


def _to_reflectance(da: DataArray) -> DataArray:
    """Convert raw Collection 2 SR DN to reflectance, clipped to valid range."""
    return (da * LANDSAT_SCALE + LANDSAT_OFFSET).clip(0.0, 1.0)


def mask_blue_white_cloud(
    ds: Dataset,
    blue_band: str = "blue",
    green_band: str = "green",
    red_band: str = "red",
    nodata_value: int = 0,
) -> Dataset:
    """Mask hard white cloud missed by CFMask using spectral indices.

    Detects bright, spectrally flat (white/grey) pixels via blue
    reflectance and a visible-band whiteness index. Operates in
    reflectance space; output values remain unscaled DN.
    QA bands are preserved unchanged.

    Args:
        ds: Dataset with unscaled Collection 2 SR bands and QA bands.
        blue_band: Name of blue band in ds.
        green_band: Name of green band in ds.
        red_band: Name of red band in ds.
        nodata_value: Fill value for masked pixels (0 = C2 SR fill convention).

    Returns:
        New dataset. Masked spectral pixels set to nodata_value. QA unchanged.
    """
    spectral_bands = [b for b in ds.data_vars if b not in qa_bands]

    required = {blue_band, green_band, red_band}
    if not required.issubset(ds.data_vars):
        missing = required - {str(b) for b in ds.data_vars}
        logger.warning(f"Hard cloud masking skipped - missing bands: {missing}")
        return ds

    blue = _to_reflectance(ds[blue_band])
    green = _to_reflectance(ds[green_band])
    red = _to_reflectance(ds[red_band])

    mean_vis = (blue + green + red) / 3.0
    whiteness = (
        abs(blue - mean_vis) + abs(green - mean_vis) + abs(red - mean_vis)
    ) / mean_vis.where(mean_vis != 0)

    cloud_mask = (blue > _CLOUD_BLUE_MIN) & (whiteness < _CLOUD_WHITENESS_MAX)

    if not cloud_mask.any():
        return ds

    masked = {
        band: ds[band].where(~cloud_mask, other=nodata_value) for band in spectral_bands
    }
    return ds.assign(masked)


def mask_nodata_clouds_saturated(
    ds: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    mask_shadow: bool = True,
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
        mask_shadow: Whether to include cloud shadow (qa_pixel bit 4).
    """
    ds = mask_nodata(ds)

    ds = mask_qa_pixel(ds, filters=filters, mask_shadow=mask_shadow)

    ds = mask_saturated(ds)

    ds = mask_blue_white_cloud(ds)

    # return erase_bad(ds, combined_mask)
    # Performance seems fine using this method (compared to erase_bad), but could be checked more closely.
    return ds


class InsufficientScenesError(LdnError):
    """Raised when there are too few timesteps to process."""


class GeoMADProcessor(Processor):
    def __init__(
        self,
        send_area_to_processor: bool = False,
        load_data_before_writing: bool = True,
        min_timesteps: int = 3,
        geomad_options: dict = {
            "num_threads": 4,
            "work_chunks": (1000, 1000),
            "maxiters": 1000,
        },
        drop_vars: list[str] = [],
        preprocessor: Processor | None = None,
        mask_clouds_kwargs: dict = {
            "filters": [("opening", 3), ("dilation", 5), ("erosion", 2)],
            "mask_shadow": True,
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
            raise InsufficientScenesError(
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


# This is a generic function used be geomad and classify tasks.
class AwsStacTask(AreaTask):
    """Area task with search + STAC creation/writing for AWS workflows."""

    def __init__(
        self,
        itempath: S3ItemPath,
        id: str,
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
        logger.info(f"Found {len(items)} items for this tile/year")
        input_data = self.loader.load(items, self.area)
        logger.info(f"Loaded {len(input_data.time.values)} items for this tile/year")

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
