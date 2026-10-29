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
from ldn.utils import LdnError


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
