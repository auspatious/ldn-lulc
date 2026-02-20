from datetime import datetime
from logging import Logger, getLogger
from typing import Iterable, Tuple

from datacube_compute import geomedian_with_mads
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AreaTask
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter, Writer
from geopandas import GeoDataFrame
import numpy as np
from odc.algo import erase_bad, mask_cleanup
from xarray import DataArray, Dataset

TaskID = str

USGS_CATALOG = "https://earth-search.aws.element84.com/v1"
USGS_COLLECTION = "landsat-c2-l2"

LANDSAT_BANDS = ["qa_pixel", "red", "green", "blue", "nir08", "swir16", "swir22"]
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


def set_stac_properties(
    input_xr: DataArray | Dataset, output_xr: DataArray | Dataset
) -> Dataset | DataArray:
    start_year = np.datetime64(input_xr.time.min().values, "Y")
    end_year = np.datetime64(input_xr.time.max().values, "Y")
    start_year_index = int(start_year.astype("int64"))
    end_year_index = int(end_year.astype("int64"))

    start_datetime = _to_utc_ms_string(start_year)
    end_datetime = _to_utc_ms_string(
        end_year + np.timedelta64(1, "Y") - np.timedelta64(1, "s")
    )

    datetime_value = start_datetime
    if start_year_index != end_year_index:
        midpoint_year_index = (start_year_index + end_year_index) // 2
        midpoint_year = np.datetime64("1970", "Y") + np.timedelta64(
            midpoint_year_index, "Y"
        )
        datetime_value = _to_utc_ms_string(midpoint_year)

    output_xr.attrs["stac_properties"] = dict(
        start_datetime=start_datetime,
        datetime=datetime_value,
        end_datetime=end_datetime,
        created=_to_utc_ms_string(np.datetime64(datetime.now())),
    )

    return output_xr


def mask_clouds(
    xr: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    include_shadow: bool = True,
) -> Dataset:
    # Only valid for LS8 and LS9, but we can still apply
    # it to LS7 data without error, it just won't mask anything.
    CIRRUS = 2
    CLOUD = 3
    CLOUD_SHADOW = 4

    fields = [CIRRUS, CLOUD]
    if include_shadow:
        fields.append(CLOUD_SHADOW)

    bitmask = 0
    for field in fields:
        bitmask |= 1 << field

    cloud_mask = xr.qa_pixel & bitmask != 0

    if filters is not None:
        cloud_mask = mask_cleanup(cloud_mask, filters)

    return erase_bad(xr, cloud_mask)


class GeoMADProcessor(Processor):
    def __init__(
        self,
        send_area_to_processor: bool = False,
        load_data_before_writing: bool = True,
        min_timesteps: int = 0,
        geomad_options: dict = {
            "num_threads": 4,
            "work_chunks": (1000, 1000),
            "maxiters": 1000,
        },
        drop_vars: list[str] = [],
        preprocessor: Processor | None = None,
        mask_clouds_kwargs: dict = {
            "filters": [("dilation", 3), ("erosion", 2)],
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

    def process(self, xr: DataArray) -> Dataset:
        if xr.time.size < self.min_timesteps:
            raise EmptyCollectionError(
                f"{xr.time.size} is less than {self.min_timesteps} timesteps"
            )

        xr = mask_clouds(xr, **self.mask_kwargs)
        data = xr.drop_vars(self.drop_vars) if len(self.drop_vars) > 0 else xr

        geomad = geomedian_with_mads(data, **self.geomad_options)

        if self.load_data_before_writing:
            geomad = geomad.compute()

        geomad["count"].odc.nodata = 0
        return set_stac_properties(data, geomad)


class AwsStacTask(AreaTask):
    """Area task with search + STAC creation/writing for AWS workflows."""

    def __init__(
        self,
        itempath: S3ItemPath,
        id: TaskID,
        area: GeoDataFrame,
        searcher: Searcher,
        loader: StacLoader,
        processor: Processor,
        post_processor: Processor | None = None,
        logger: Logger = getLogger(),
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
        input_data = self.loader.load(items, self.area)

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
