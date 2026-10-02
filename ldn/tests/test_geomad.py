import numpy as np
import xarray as xr

from ldn.geomad import LANDSAT_BANDS, GeoMADProcessor, _set_stac_properties

EXPECTED_BANDS = [
    "nir08",
    "red",
    "green",
    "blue",
    "swir16",
    "swir22",
    "smad",
    "bcmad",
    "emad",
    "count",
]


def _make_landsat_input(n_times: int, size: int) -> xr.Dataset:
    """Build a tiny multi-timestep Landsat-like dataset with all required bands."""
    coords = {
        "time": np.array([f"2020-0{i + 1}-15" for i in range(n_times)], dtype="datetime64[ns]"),
        "y": np.arange(size, dtype="float64"),
        "x": np.arange(size, dtype="float64"),
    }
    rng = np.random.default_rng(42)
    data_vars = {}
    for band in LANDSAT_BANDS:
        if band in ("qa_pixel", "qa_radsat"):
            data_vars[band] = (
                ["time", "y", "x"],
                np.zeros((n_times, size, size), dtype="uint16"),
            )
        else:
            data_vars[band] = (
                ["time", "y", "x"],
                rng.integers(7273, 43636, size=(n_times, size, size), dtype="uint16"),
            )
    return xr.Dataset(data_vars, coords=coords)


def test_geomad_processor_output_has_expected_bands_nodata_and_dtype() -> None:
    """GeoMADProcessor output must contain exactly EXPECTED_BANDS, and the correct nodata value and dtype."""
    input_ds = _make_landsat_input(n_times=3, size=4)

    processor = GeoMADProcessor(
        year="2020",
        load_data_before_writing=False,
        min_timesteps=3,
        drop_vars=["qa_pixel", "qa_radsat"],
        mask_clouds_kwargs={"filters": None, "mask_shadow": False},
    )
    result = processor.process(input_ds)

    assert set(result.data_vars) == set(EXPECTED_BANDS)
    assert result["red"].attrs["nodata"] == 0
    assert result["red"].dtype == np.uint16
    assert result["count"].attrs["nodata"] == 9999
    assert result["count"].dtype == np.uint16
    assert np.isnan(result["emad"].attrs["nodata"])
    assert result["emad"].dtype == np.float32


# Occurs for years >2012, where only 1 year of data is used.
def test_set_stac_properties_datetime_same_year() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["2020-03-01", "2020-11-15"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr, "2020")
    props = result.attrs["stac_properties"]

    assert props["start_datetime"] == "2020-01-01T00:00:00Z"
    assert props["datetime"] == "2020-06-30T00:00:00Z"
    assert props["end_datetime"] == "2020-12-31T23:59:59Z"


# Occurs for years <=2012, where a 1-year buffer is used (so 3-year span).
def test_set_stac_properties_datetime_three_year_span() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["1999-02-10", "2001-10-20"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr, "2000")
    props = result.attrs["stac_properties"]

    assert props["start_datetime"] == "1999-01-01T00:00:00Z"
    assert props["datetime"] == "2000-06-30T00:00:00Z"
    assert props["end_datetime"] == "2001-12-31T23:59:59Z"


# This could happen if a 3-year span only finds data in 2 years. Uncommon. The passed year is used, not inferred.
def test_set_stac_properties_datetime_uses_passed_year_when_years_differ() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["2020-03-01", "2021-11-15"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr, "2021")
    props = result.attrs["stac_properties"]

    assert props["start_datetime"] == "2020-01-01T00:00:00Z"
    assert props["datetime"] == "2021-06-30T00:00:00Z"
    assert props["end_datetime"] == "2021-12-31T23:59:59Z"
