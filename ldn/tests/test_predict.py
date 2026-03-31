import numpy as np
import xarray as xr

from ldn.classify import calculate_indices, scale_offset_landsat


def _make_dataset(values: dict[str, list[list[float]]]) -> xr.Dataset:
    """Create a simple 2D xarray Dataset from a dict of band name to 2D values."""
    data_vars = {}
    for name, vals in values.items():
        data_vars[name] = xr.DataArray(
            np.array(vals, dtype="float32"),
            dims=["y", "x"],
        )
    return xr.Dataset(data_vars)


# scale_offset_landsat


class TestScaleOffsetLandsat:
    def test_scales_values_to_float_range(self):
        """Valid pixel values are scaled to the [0, 1] range."""
        # 10000 * 0.0000275 - 0.2 = 0.075
        ds = _make_dataset({"red": [[10000]]})
        result = scale_offset_landsat(ds)
        expected = 10000 * 0.0000275 - 0.2
        np.testing.assert_almost_equal(result["red"].values[0, 0], expected, decimal=5)

    def test_output_dtype_is_float32(self):
        """Scaled bands should be float32."""
        ds = _make_dataset({"red": [[10000]], "green": [[8000]]})
        result = scale_offset_landsat(ds)
        for band in result.data_vars:
            assert result[band].dtype == np.float32

    def test_nodata_zero_becomes_nan(self):
        """Pixels with raw value 0 (nodata) should become NaN."""
        ds = _make_dataset({"red": [[0]]})
        result = scale_offset_landsat(ds)
        assert np.isnan(result["red"].values[0, 0])

    def test_nodata_65535_becomes_nan(self):
        """Pixels with raw value 65535 (fill value) should become NaN."""
        ds = _make_dataset({"red": [[65535]]})
        result = scale_offset_landsat(ds)
        assert np.isnan(result["red"].values[0, 0])

    def test_clips_negative_to_zero(self):
        """Values that scale below 0 should be clipped to 0."""
        # 1 * 0.0000275 - 0.2 = -0.1999725 -> clipped to 0.0
        ds = _make_dataset({"red": [[1]]})
        result = scale_offset_landsat(ds)
        assert result["red"].values[0, 0] == 0.0

    def test_clips_high_to_one(self):
        """Values that scale above 1 should be clipped to 1."""
        # 50000 * 0.0000275 - 0.2 = 1.175 -> clipped to 1.0
        ds = _make_dataset({"red": [[50000]]})
        result = scale_offset_landsat(ds)
        assert result["red"].values[0, 0] == 1.0

    def test_multiple_bands(self):
        """All reflectance bands in the dataset should be scaled."""
        ds = _make_dataset({
            "red": [[10000]],
            "green": [[10000]],
            "blue": [[10000]],
        })
        result = scale_offset_landsat(ds)
        expected = 10000 * 0.0000275 - 0.2
        for band in ["red", "green", "blue"]:
            np.testing.assert_almost_equal(
                result[band].values[0, 0], expected, decimal=5
            )

    def test_skips_excluded_bands(self):
        """Bands like 'count', 'emad', 'smad', 'bcmad' should not be scaled."""
        ds = _make_dataset({
            "red": [[10000]],
            "count": [[42]],
            "emad": [[100]],
        })
        result = scale_offset_landsat(ds)
        assert result["count"].values[0, 0] == 42
        assert result["emad"].values[0, 0] == 100

    def test_mixed_valid_and_nodata(self):
        """A band with both valid pixels and nodata should scale valid and NaN nodata."""
        ds = _make_dataset({"red": [[10000, 0, 65535, 8000]]})
        result = scale_offset_landsat(ds)
        values = result["red"].values[0]
        assert not np.isnan(values[0])
        assert np.isnan(values[1])
        assert np.isnan(values[2])
        assert not np.isnan(values[3])


# calculate_indices

EXPECTED_INDEX_BANDS = ["ndvi", "ndwi", "mndwi", "ndti", "bsi", "mbi", "baei", "bui"]


def _make_geomad_dataset(
    nir08: float,
    red: float,
    green: float,
    blue: float,
    swir16: float,
    swir22: float,
) -> xr.Dataset:
    """Create a minimal GeoMAD dataset with the required reflectance bands."""
    return _make_dataset({
        "nir08": [[nir08]],
        "red": [[red]],
        "green": [[green]],
        "blue": [[blue]],
        "swir16": [[swir16]],
        "swir22": [[swir22]],
    })


class TestCalculateIndices:
    def test_adds_all_index_bands(self):
        """All expected index bands should be added to the dataset."""
        ds = _make_geomad_dataset(0.3, 0.1, 0.2, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        for band in EXPECTED_INDEX_BANDS:
            assert band in result.data_vars, f"Missing band: {band}"

    def test_preserves_original_bands(self):
        """Original reflectance bands should not be removed."""
        ds = _make_geomad_dataset(0.3, 0.1, 0.2, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        for band in ["nir08", "red", "green", "blue", "swir16", "swir22"]:
            assert band in result.data_vars

    def test_ndvi_formula(self):
        """NDVI = (nir - red) / (nir + red)."""
        nir, red = 0.4, 0.1
        ds = _make_geomad_dataset(nir, red, 0.2, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        expected = (nir - red) / (nir + red)
        np.testing.assert_almost_equal(result["ndvi"].values[0, 0], expected, decimal=5)

    def test_ndwi_formula(self):
        """NDWI = (green - nir) / (green + nir)."""
        nir, green = 0.3, 0.5
        ds = _make_geomad_dataset(nir, 0.1, green, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        expected = (green - nir) / (green + nir)
        np.testing.assert_almost_equal(result["ndwi"].values[0, 0], expected, decimal=5)

    def test_mndwi_formula(self):
        """MNDWI = (green - swir1) / (green + swir1)."""
        green, swir1 = 0.4, 0.2
        ds = _make_geomad_dataset(0.3, 0.1, green, 0.15, swir1, 0.2)
        result = calculate_indices(ds)
        expected = (green - swir1) / (green + swir1)
        np.testing.assert_almost_equal(result["mndwi"].values[0, 0], expected, decimal=5)

    def test_bui_equals_ndbi_minus_ndvi(self):
        """BUI = NDBI - NDVI, where NDBI = (swir1 - nir) / (swir1 + nir)."""
        nir, red, swir1 = 0.3, 0.1, 0.25
        ds = _make_geomad_dataset(nir, red, 0.2, 0.15, swir1, 0.2)
        result = calculate_indices(ds)
        ndbi = (swir1 - nir) / (swir1 + nir)
        ndvi = (nir - red) / (nir + red)
        expected = ndbi - ndvi
        np.testing.assert_almost_equal(result["bui"].values[0, 0], expected, decimal=5)

    def test_indices_in_expected_range(self):
        """Normalised indices should be in [-1, 1] for typical reflectance values."""
        ds = _make_geomad_dataset(0.3, 0.1, 0.2, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        for band in ["ndvi", "ndwi", "mndwi", "ndti", "bsi"]:
            val = float(result[band].values[0, 0])
            assert -1.0 <= val <= 1.0, f"{band} = {val} is out of [-1, 1]"

    def test_nan_input_propagates(self):
        """NaN in an input band should propagate to all indices that use it."""
        ds = _make_geomad_dataset(np.nan, 0.1, 0.2, 0.15, 0.25, 0.2)
        result = calculate_indices(ds)
        # nir08 is NaN, so ndvi, ndwi, bsi, mbi, bui should all be NaN
        for band in ["ndvi", "ndwi", "bsi", "mbi", "bui"]:
            assert np.isnan(result[band].values[0, 0]), f"{band} should be NaN"

    def test_division_by_zero_produces_nan(self):
        """When both bands in a ratio are 0, result should be NaN."""
        ds = _make_geomad_dataset(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        result = calculate_indices(ds)
        # All normalised difference indices divide by zero
        for band in ["ndvi", "ndwi", "mndwi", "ndti", "bsi"]:
            val = float(result[band].values[0, 0])
            assert np.isnan(val), f"{band} = {val}"



# TODO: Add tests for further prediction functions.