import numpy as np
import xarray as xr

from ldn.geomad import set_stac_properties


def test_set_stac_properties_datetime_same_year() -> None:
    input_xr = xr.Dataset(
        coords={
            "time": np.array(
                ["2020-03-01", "2020-11-15"], dtype="datetime64[ns]"
            )
        }
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
        coords={
            "time": np.array(
                ["2020-03-01", "2021-11-15"], dtype="datetime64[ns]"
            )
        }
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
        coords={
            "time": np.array(
                ["1999-02-10", "2001-10-20"], dtype="datetime64[ns]"
            )
        }
    )
    output_xr = xr.Dataset()

    result = set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    expected_midpoint = np.datetime_as_string(
        np.datetime64("2000", "Y"), unit="ms", timezone="UTC"
    )

    assert props["datetime"] == expected_midpoint
