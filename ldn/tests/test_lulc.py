from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pystac import Asset, Item
from pystac.extensions.classification import ClassificationExtension
from sklearn.ensemble import RandomForestClassifier

from ldn.lulc import StacGeoparquetSearcher, _add_lulc_stac_metadata, do_prediction, geomad_searcher
from ldn.raster import calculate_indices, scale_offset_landsat
from ldn.utils import LdnError


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
        ds = _make_dataset(
            {
                "red": [[10000]],
                "green": [[10000]],
                "blue": [[10000]],
            }
        )
        result = scale_offset_landsat(ds)
        expected = 10000 * 0.0000275 - 0.2
        for band in ["red", "green", "blue"]:
            np.testing.assert_almost_equal(result[band].values[0, 0], expected, decimal=5)

    def test_skips_excluded_bands(self):
        """Bands like 'count', 'emad', 'smad', 'bcmad' should not be scaled."""
        ds = _make_dataset(
            {
                "red": [[10000]],
                "count": [[42]],
                "emad": [[100]],
            }
        )
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
    return _make_dataset(
        {
            "nir08": [[nir08]],
            "red": [[red]],
            "green": [[green]],
            "blue": [[blue]],
            "swir16": [[swir16]],
            "swir22": [[swir22]],
        }
    )


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

    def test_division_by_zero_produces_zero(self):
        """When both bands in a normalised difference are 0 (valid dark pixel), result should be 0, not NaN."""
        ds = _make_geomad_dataset(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        result = calculate_indices(ds)
        for band in ["ndvi", "ndwi", "mndwi", "ndti", "bsi"]:
            val = float(result[band].values[0, 0])
            assert val == 0, f"{band} = {val}"


class TestGeomadSearcher:
    def test_many_region_index_with_padded_id(self):
        """Non-Pacific tile without single region uses the combined index and the writer's zero-padded ID."""
        searcher = geomad_searcher(
            "us-west-2.opendata.source.coop", "non-pacific", "ci", False, "ls", "0-2-1", "2020", (334, 92)
        )
        assert searcher._item_id == "ci_ls_geomad_334_092_2020"
        assert searcher._url.endswith("/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet")

    def test_single_region_index(self):
        """Single region uses the owner's own index."""
        searcher = geomad_searcher("dep-public-staging", "pacific", "dep", True, "ls", "0-2-1", "2020", (58, 43))
        assert searcher._item_id == "dep_ls_geomad_058_043_2020"
        assert searcher._url.endswith("/dep-public-staging/dep_ls_geomad/0-2-1/dep_ls_geomad.parquet")

    def test_region_owner_when_no_override(self):
        """Without an owner override the ID uses the region's owner."""
        searcher = geomad_searcher("dep-public-staging", "non-pacific", None, False, "ls", "0-2-1", "2020", (119, 126))
        assert searcher._item_id == "ci_ls_geomad_119_126_2020"

    @pytest.mark.parametrize("region,owner,tile", [("pacific", "dep", (66, 20)), ("non-pacific", "ci", (119, 126))])
    def test_neighbour_ids_are_the_8_surrounding_tiles(self, region, owner, tile):
        """The padded tile overlaps its 8 neighbours, including across the antimeridian (Pacific 066_020)."""
        searcher = geomad_searcher("dep-public-staging", region, owner, True, "ls", "0-2-1", "2020", tile)
        x, y = tile
        expected = {
            f"{owner}_ls_geomad_{x + dx:03d}_{y + dy:03d}_2020"
            for dx in [-1, 0, 1]
            for dy in [-1, 0, 1]
            if (dx, dy) != (0, 0)
        }
        assert set(searcher._neighbour_ids) == expected


class TestStacGeoparquetSearcher:
    @staticmethod
    def _doc(item_id: str) -> dict:
        return Item(id=item_id, geometry=None, bbox=None, datetime=datetime(2020, 6, 30), properties={}).to_dict()

    def test_tile_item_first_and_missing_neighbours_skipped(self):
        searcher = StacGeoparquetSearcher("https://example.com/x.parquet", "tile", ["n1", "n2"])
        with patch("ldn.lulc.search_sync", return_value=[self._doc("n1"), self._doc("tile")]):
            items = searcher.search()
        assert [item.id for item in items] == ["tile", "n1"]

    def test_missing_tile_item_raises(self):
        searcher = StacGeoparquetSearcher("https://example.com/x.parquet", "tile", ["n1"])
        with patch("ldn.lulc.search_sync", return_value=[self._doc("n1")]):
            with pytest.raises(LdnError, match="tile not found"):
                searcher.search()


# TODO: Add tests for further prediction functions.


def test_add_lulc_stac_metadata():
    """Model link, classification classes and asset titles are added, with nodata using the output nodata value."""
    item = Item(id="t", geometry=None, bbox=None, datetime=datetime(2020, 1, 1), properties={})
    for name in ["classification", "probability_1", "probability_5"]:
        item.add_asset(name, Asset(href=f"{name}.tif"))
    model_path = "https://example.com/dep_ls_lulc/models/0-1-1/2020/lulc_random_forest_model_2020.joblib"
    _add_lulc_stac_metadata(item, model_path, nodata_value=0)

    model_link = item.get_single_link("related")
    assert (model_link.href, model_link.title) == (model_path, "LULC classification model 0-1-1")
    assert item.assets["probability_1"].title == "Probability of Tree cover (%)"
    assert item.assets["probability_5"].title == "Probability of Built-up (%)"
    assert ClassificationExtension.get_schema_uri() in item.stac_extensions
    classes = {c.name: c for c in ClassificationExtension.ext(item.assets["classification"]).classes}
    assert len(classes) == 8
    assert (classes["no_data"].value, classes["no_data"].nodata) == (0, True)
    assert (classes["tree_cover"].value, classes["tree_cover"].title) == (1, "Tree cover")
    assert classes["tree_cover"].color_hint == "006400"


def test_probability_bands_are_named_by_class_value():
    """Bands follow the model's classes, so a class missing from training doesn't shift the names."""
    features = pd.DataFrame({"a": [0.0, 1.0, 0.0, 1.0], "b": [0.0, 0.0, 1.0, 1.0]})
    model = RandomForestClassifier(n_estimators=2, random_state=0).fit(features, [1, 3, 1, 3])
    values = np.array([[0.0, 1.0]], dtype=np.float32)
    ds = xr.Dataset({"a": (("y", "x"), values), "b": (("y", "x"), values)}, coords={"y": [0], "x": [0, 1]})
    _, probabilities = do_prediction(ds, model, 0, 255)
    assert list(probabilities) == ["probability_1", "probability_3"]
