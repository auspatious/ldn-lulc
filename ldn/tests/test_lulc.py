from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr
from pystac import Asset, Item
from pystac.extensions.classification import ClassificationExtension
from sklearn.ensemble import RandomForestClassifier

from ldn.lulc import _add_lulc_stac_metadata, do_prediction, geomad_searcher


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
