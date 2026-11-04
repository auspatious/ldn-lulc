from datetime import datetime
from unittest.mock import patch

import dask
import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pystac import Asset, Item
from pystac.extensions.classification import ClassificationExtension
from sklearn.ensemble import RandomForestClassifier

from ldn.lulc import GeomadSearcher, _add_lulc_stac_metadata, do_prediction, gap_fill, source_years
from ldn.utils import LdnError


class TestGeomadSearcher:
    def test_many_region_index_with_padded_id(self):
        """Non-Pacific tile without single region uses the combined index and the writer's zero-padded ID."""
        searcher = GeomadSearcher(
            "us-west-2.opendata.source.coop", "non-pacific", "ci", False, "ls", "0-2-1", "2020", (334, 92), 0
        )
        assert searcher._item_id == "ci_ls_geomad_334_092_2020"
        assert searcher._url.endswith("/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet")

    def test_single_region_index(self):
        """Single region uses the owner's own index."""
        searcher = GeomadSearcher("dep-public-staging", "pacific", "dep", True, "ls", "0-2-1", "2020", (58, 43), 0)
        assert searcher._item_id == "dep_ls_geomad_058_043_2020"
        assert searcher._url.endswith("/dep-public-staging/dep_ls_geomad/0-2-1/dep_ls_geomad.parquet")

    def test_region_owner_when_no_override(self):
        """Without an owner override the ID uses the region's owner."""
        searcher = GeomadSearcher(
            "dep-public-staging", "non-pacific", None, False, "ls", "0-2-1", "2020", (119, 126), 0
        )
        assert searcher._item_id == "ci_ls_geomad_119_126_2020"

    @pytest.mark.parametrize("region,owner,tile", [("pacific", "dep", (66, 20)), ("non-pacific", "ci", (119, 126))])
    def test_neighbour_ids_are_the_8_surrounding_tiles(self, region, owner, tile):
        """The padded tile overlaps its 8 neighbours, including across the antimeridian (Pacific 066_020)."""
        searcher = GeomadSearcher("dep-public-staging", region, owner, True, "ls", "0-2-1", "2020", tile, 0)
        x, y = tile
        expected = {
            f"{owner}_ls_geomad_{x + dx:03d}_{y + dy:03d}_2020"
            for dx in [-1, 0, 1]
            for dy in [-1, 0, 1]
            if (dx, dy) != (0, 0)
        }
        assert set(searcher._other_ids) == expected

    def test_gap_fill_years_add_the_same_tiles_for_each_year(self):
        searcher = GeomadSearcher("dep-public-staging", "pacific", "dep", True, "ls", "0-2-1", "2020", (58, 43), 2)
        assert searcher._item_id == "dep_ls_geomad_058_043_2020"
        assert len(searcher._other_ids) == 9 * 5 - 1
        assert {"dep_ls_geomad_058_043_2018", "dep_ls_geomad_057_042_2022"} <= set(searcher._other_ids)

    @staticmethod
    def _doc(item_id: str) -> dict:
        """STAC item dict as returned by rustac."""
        return Item(id=item_id, geometry=None, bbox=None, datetime=datetime(2020, 6, 30), properties={}).to_dict()

    def test_tile_item_first_and_missing_neighbours_skipped(self):
        searcher = GeomadSearcher("dep-public-staging", "pacific", "dep", True, "ls", "0-2-1", "2020", (58, 43), 0)
        neighbour = "dep_ls_geomad_057_043_2020"
        with patch("ldn.lulc.search_sync", return_value=[self._doc(neighbour), self._doc(searcher._item_id)]):
            items = searcher.search()
        assert [item.id for item in items] == [searcher._item_id, neighbour]

    def test_missing_tile_item_raises(self):
        searcher = GeomadSearcher("dep-public-staging", "pacific", "dep", True, "ls", "0-2-1", "2020", (58, 43), 0)
        with patch("ldn.lulc.search_sync", return_value=[self._doc("dep_ls_geomad_057_043_2020")]):
            with pytest.raises(LdnError, match="dep_ls_geomad_058_043_2020 not found"):
                searcher.search()


# TODO: Add tests for further prediction functions.


class TestGapFill:
    """Pixels: 0 valid, 1 fillable from 1 year away, 2 only from 2 years away, 3 never fillable.

    gap_filled holds the signed year offset where a pixel was filled (-1 = year before), 0 (nodata) elsewhere.
    """

    @staticmethod
    def _data() -> xr.Dataset:
        years = [2018, 2019, 2020, 2021, 2022]
        # Raw reflectance (0 = nodata, declared like the GeoMAD STAC items) and a MAD band (NaN = nodata), as GeoMAD
        # loads.
        red = np.array(
            [
                [10, 11, 12, 0],  # 2018
                [20, 21, 22, 0],  # 2019: pixel 2 red is valid but its emad is NaN, so the pixel isn't used
                [30, 0, 0, 0],  # 2020
                [40, 41, 0, 0],  # 2021
                [50, 51, 52, 0],  # 2022
            ],
            dtype=np.uint16,
        )
        emad = red.astype(np.float32)
        emad[1, 2] = np.nan
        emad[red == 0] = np.nan
        time = pd.to_datetime([f"{year}-06-30" for year in years])
        dims = ("time", "y", "x")
        return xr.Dataset(
            {"red": (dims, red[:, None, :], {"nodata": 0}), "emad": (dims, emad[:, None, :])}, coords={"time": time}
        )

    def test_fills_whole_pixels_from_nearest_valid_year(self):
        filled, gap_filled = gap_fill(self._data(), 2020, 2)
        assert gap_filled.values.tolist() == [[0, -1, -2, 0]]
        assert gap_filled.dtype == np.int8
        # Pixel 1 from 2019 (earlier wins a tie), pixel 2 from 2018, all bands from the same year.
        assert filled["red"].values.tolist() == [[30, 21, 12, 0]]
        np.testing.assert_array_equal(filled["emad"].values, [[30, 21, 12, np.nan]])
        assert "time" not in filled.dims
        # 2021 and 2022 weren't used.
        assert source_years(gap_filled, 2020) == {2018, 2019, 2020}

    def test_year_after_gives_positive_offset(self):
        """Pixel 2's 2019 is unusable, so a valid 2021 (+1) wins over 2018 (-2)."""
        data = self._data()
        data["red"][3, 0, 2] = 42
        data["emad"][3, 0, 2] = 42.0
        filled, gap_filled = gap_fill(data, 2020, 2)
        assert gap_filled.values.tolist() == [[0, -1, 1, 0]]
        assert filled["red"].values.tolist() == [[30, 21, 42, 0]]

    def test_max_years_limits_the_search(self):
        _, gap_filled = gap_fill(self._data(), 2020, 1)
        assert gap_filled.values.tolist() == [[0, -1, 0, 0]]

    def test_stops_reading_years_once_gaps_are_filled(self):
        """2019 fills every gap, so 2021 and 2022 must never be read (their data raises if computed)."""
        data = self._data().isel(x=[0, 1])

        def unreadable():
            raise AssertionError("year was read")

        lazy = da.from_delayed(dask.delayed(unreadable)(), shape=(2, 1, 2), dtype=np.uint16)
        data["red"] = data["red"].copy(data=da.concatenate([da.from_array(data["red"].values[:3]), lazy]))
        _, gap_filled = gap_fill(data, 2020, 2)
        assert gap_filled.values.tolist() == [[0, -1]]

    def test_no_gaps_returns_the_year_unchanged(self):
        data = self._data()
        data["red"][2] = 1
        data["emad"][2] = 1.0
        filled, gap_filled = gap_fill(data, 2020, 2)
        assert gap_filled.values.tolist() == [[0, 0, 0, 0]]
        assert filled["red"].values.tolist() == [[1, 1, 1, 1]]
        assert source_years(gap_filled, 2020) == {2020}


class TestDoPredictionFeatureCheck:
    @pytest.fixture
    def model(self) -> RandomForestClassifier:
        features = pd.DataFrame({"a": [0.0, 1.0, 0.0, 1.0], "b": [0.0, 0.0, 1.0, 1.0]})
        return RandomForestClassifier(n_estimators=2, random_state=0).fit(features, [1, 2, 1, 2])

    @staticmethod
    def _features(*names: str) -> xr.Dataset:
        return xr.Dataset({name: (("y", "x"), np.zeros((2, 2), dtype=np.float32)) for name in names})

    def test_matching_features_predict(self, model):
        classification, _ = do_prediction(self._features("b", "a"), model, 0, 255)
        assert classification.shape == (2, 2)

    @pytest.mark.parametrize("names,message", [(("a",), "Missing: \\['b'\\]"), (("a", "b", "c"), "extra: \\['c'\\]")])
    def test_missing_or_extra_features_raise(self, model, names, message):
        with pytest.raises(LdnError, match=message):
            do_prediction(self._features(*names), model, 0, 255)


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
