from typing import Iterable, Tuple

from datacube_compute import geomedian_with_mads
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.processors import Processor
from dep_tools.stac_utils import set_stac_properties
from xarray import DataArray, Dataset
from odc.algo import erase_bad, mask_cleanup


USGS_CATALOG = "https://earth-search.aws.element84.com/v1"
USGS_COLLECTION = "landsat-c2-l2"

LANDSAT_BANDS = ["qa_pixel", "red", "green", "blue", "nir08", "swir16", "swir22"]
LANDSAT_SCALE = 0.0000275
LANDSAT_OFFSET = -0.2


def http_to_s3_url(http_url):
    """Convert a USGS HTTP URL to an S3 URL"""
    s3_url = http_url.replace(
        "https://landsatlook.usgs.gov/data", "s3://usgs-landsat"
    ).rstrip(":1")
    return s3_url


def mask_clouds(
    xr: Dataset,
    filters: Iterable[Tuple[str, int]] | None = None,
    include_shadow: bool = True,
) -> Dataset:

    NODATA = 1
    CIRRUS = 2  # Only valid for LS8 and LS9, but we can still apply
    # it to LS7 data without error, it just won't mask anything.
    CLOUD = 3
    CLOUD_SHADOW = 4

    # nodata = xr.qa_pixel == xr.qa_pixel.odc.nodata

    fields = [CIRRUS, CLOUD]
    if include_shadow:
        fields.append(CLOUD_SHADOW)

    bitmask = 0
    for field in fields:
        bitmask |= 1 << field

    cloud_mask = xr.qa_pixel & bitmask != 0

    if filters is not None:
        cloud_mask = mask_cleanup(cloud_mask, filters)

    # mask = nodata | cloud_mask

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
        # Raise an exception if there's not enough data
        if xr.time.size < self.min_timesteps:
            raise EmptyCollectionError(
                f"{xr.time.size} is less than {self.min_timesteps} timesteps"
            )

        xr = mask_clouds(xr, **self.mask_kwargs)

        data = xr

        if len(self.drop_vars) > 0:
            data = data.drop_vars(self.drop_vars)

        geomad = geomedian_with_mads(data, **self.geomad_options)

        if self.load_data_before_writing:
            geomad = geomad.compute()

        # Add nodata as 0 to the count variable
        geomad["count"].odc.nodata = 0

        output = set_stac_properties(data, geomad)

        return output


class GeoMADLandsatProcessor(GeoMADProcessor):
    def __init__(
        self,
        mask_clouds_kwargs: dict = {
            "filters": [("dilation", 3), ("erosion", 2)],
            "include_shadow": True,
        },
        drop_vars: list[str] = ["qa_pixel"],
        **kwargs,
    ) -> None:
        super().__init__(
            mask_clouds_kwargs=mask_clouds_kwargs,
            drop_vars=drop_vars,
            **kwargs,
        )


class GeoMADPostProcessor(Processor):
    def __init__(
        self,
        vars: list[str] = [],
        drop_vars: list[str] = [],
        scale: float | None = None,
        offset: float | None = None,
    ):
        self._vars = [v for v in vars if v not in drop_vars]
        self._scale = scale
        self._offset = offset

    def process(self, xr: Dataset):
        if len(self._vars) != 0:
            for var in self._vars:
                if self._scale is not None:
                    xr[var].attrs["scales"] = self._scale
                if self._offset is not None:
                    xr[var].attrs["offsets"] = self._offset
        return xr
