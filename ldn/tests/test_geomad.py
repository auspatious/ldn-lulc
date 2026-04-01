from unittest.mock import patch

import numpy as np
import xarray as xr

from ldn.geomad import GeoMADProcessor, LANDSAT_BANDS, set_stac_properties

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
        "time": np.array(
            [f"2020-0{i + 1}-15" for i in range(n_times)], dtype="datetime64[ns]"
        ),
        "y": np.arange(size, dtype="float64"),
        "x": np.arange(size, dtype="float64"),
    }
    rng = np.random.default_rng(42)
    data_vars = {}
    for band in LANDSAT_BANDS:
        if band == "qa_pixel":
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


def _fake_geomedian_with_mads(data, **kwargs):
    """Mimic geomedian_with_mads: median each input band, add MAD stats and count."""
    median = data.median(dim="time")
    ones = xr.DataArray(
        np.ones((data.sizes["y"], data.sizes["x"]), dtype="float32"),
        dims=["y", "x"],
        coords={"y": data.y, "x": data.x},
    )
    median["smad"] = ones
    median["bcmad"] = ones
    median["emad"] = ones
    median["count"] = xr.DataArray(
        np.full(
            (data.sizes["y"], data.sizes["x"]), data.sizes.get("time", 1), dtype="int16"
        ),
        dims=["y", "x"],
        coords={"y": data.y, "x": data.x},
    )
    return median


@patch("ldn.geomad.geomedian_with_mads", side_effect=_fake_geomedian_with_mads)
def test_geomad_processor_output_has_expected_bands(mock_geomad) -> None:
    """GeoMADProcessor output must contain exactly EXPECTED_BANDS."""
    input_ds = _make_landsat_input(n_times=3, size=4)

    processor = GeoMADProcessor(
        load_data_before_writing=False,
        drop_vars=["qa_pixel"],
        mask_clouds_kwargs={"filters": None, "include_shadow": False},
    )
    result = processor.process(input_ds)

    assert set(result.data_vars) == set(EXPECTED_BANDS)


def test_set_stac_properties_datetime_same_year() -> None:
    input_xr = xr.Dataset(
        coords={"time": np.array(["2020-03-01", "2020-11-15"], dtype="datetime64[ns]")}
    )
    output_xr = xr.Dataset()

    result = set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    expected_start = np.datetime_as_string(
        np.datetime64("2020", "Y"), unit="ms", timezone="UTC"
    )

    assert props["start_datetime"] == expected_start
    assert props["datetime"] == expected_start


def test_set_stac_properties_datetime_midpoint_when_years_differ() -> None:
    input_xr = xr.Dataset(
        coords={"time": np.array(["2020-03-01", "2021-11-15"], dtype="datetime64[ns]")}
    )
    output_xr = xr.Dataset()

    result = set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    expected_midpoint = np.datetime_as_string(
        np.datetime64("2020", "Y"), unit="ms", timezone="UTC"
    )

    assert props["datetime"] == expected_midpoint


def test_set_stac_properties_datetime_three_year_span() -> None:
    input_xr = xr.Dataset(
        coords={"time": np.array(["1999-02-10", "2001-10-20"], dtype="datetime64[ns]")}
    )
    output_xr = xr.Dataset()

    result = set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    expected_midpoint = np.datetime_as_string(
        np.datetime64("2000", "Y"), unit="ms", timezone="UTC"
    )
    expected_start = np.datetime_as_string(
        np.datetime64("1999", "Y"), unit="ms", timezone="UTC"
    )
    expected_end = np.datetime_as_string(
        np.datetime64("2002", "Y") - np.timedelta64(1, "s"),
        unit="ms",
        timezone="UTC",
    )

    assert props["start_datetime"] == expected_start
    assert props["datetime"] == expected_midpoint
    assert props["end_datetime"] == expected_end
