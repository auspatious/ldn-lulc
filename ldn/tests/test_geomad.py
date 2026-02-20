import numpy as np
import xarray as xr

from ldn.geomad import set_stac_properties
from ldn.geomad import GeoMADPostProcessor


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


def test_geomad_postprocessor_normalizes_stac_properties() -> None:
    ds = xr.Dataset()
    ds.attrs["stac_properties"] = {
        "start_datetime": "1999-01-01T00:00:00.000Z",
        "datetime": "1999-01-01T00:00:00Z",
        "end_datetime": "2001-12-31T23:59:59Z",
        "created": "2026-02-20T03:35:41.838738Z",
    }

    out = GeoMADPostProcessor().process(ds)
    props = out.attrs["stac_properties"]

    assert props["start_datetime"] == "1999-01-01T00:00:00.000Z"
    assert props["datetime"] == "2000-01-01T00:00:00.000Z"
    assert props["end_datetime"] == "2001-12-31T23:59:59.000Z"
