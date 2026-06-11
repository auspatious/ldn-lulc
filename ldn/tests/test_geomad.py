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
        load_data_before_writing=False,
        min_timesteps=3,
        drop_vars=["qa_pixel", "qa_radsat"],
        mask_clouds_kwargs={"filters": None, "mask_shadow": False},
    )
    result = processor.process(input_ds)

    assert set(result.data_vars) == set(EXPECTED_BANDS)
    assert result["red"].attrs["nodata"] == 0
    assert result["red"].dtype == np.uint16
    assert np.isnan(result["emad"].attrs["nodata"])
    assert result["emad"].dtype == np.float32


def test_set_stac_properties_datetime_same_year() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["2020-03-01", "2020-11-15"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    assert props["start_datetime"] == "2020-01-01T00:00:00Z"
    assert props["datetime"] == "2020-06-30T00:00:00Z"
    assert props["end_datetime"] == "2020-12-31T23:59:59Z"
    assert "ldn:observation_start" not in props
    assert "ldn:observation_end" not in props


def test_set_stac_properties_datetime_midpoint_when_years_differ() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["2020-03-01", "2021-11-15"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    assert props["datetime"] == "2020-06-30T00:00:00Z"
    assert props["start_datetime"] == "2020-01-01T00:00:00Z"
    assert props["end_datetime"] == "2020-12-31T23:59:59Z"
    assert props["ldn:observation_start"] == "2020-01-01T00:00:00Z"
    assert props["ldn:observation_end"] == "2021-12-31T23:59:59Z"


def test_set_stac_properties_datetime_three_year_span() -> None:
    input_xr = xr.Dataset(coords={"time": np.array(["1999-02-10", "2001-10-20"], dtype="datetime64[ns]")})
    output_xr = xr.Dataset()

    result = _set_stac_properties(input_xr, output_xr)
    props = result.attrs["stac_properties"]

    assert props["datetime"] == "2000-06-30T00:00:00Z"
    assert props["start_datetime"] == "2000-01-01T00:00:00Z"
    assert props["end_datetime"] == "2000-12-31T23:59:59Z"
    assert props["ldn:observation_start"] == "1999-01-01T00:00:00Z"
    assert props["ldn:observation_end"] == "2001-12-31T23:59:59Z"


# TODO: Add a test that an existing task without overwrite flag skips.
# TODO: Test that an existing task with overwrite flag overwrites.
# TODO: Test that a not-existing task without overwrite flag processes.

# # If we don't want to overwrite, and the destination file already exists, skip it
# # Use the write client to check if the item already exists at the destination, since it may have different creds.
# if not overwrite and object_exists(bucket, stac_key, client=aws_client_to_use):
#     typer.echo(f"Item already exists at {stac_document}, skipping.")
#     return
# else:
#     if not overwrite:
#         typer.echo(f"Item does not exist at {stac_document}, processing tile.")
