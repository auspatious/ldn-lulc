import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cogeo_mosaic.mosaic import MosaicJSON

from ldn.cli import (
    _build_mosaic_for_year,
    _extract_years,
    _find_stac_items_s3,
    _load_stac_docs,
    _run_index,
    _stac_self_link,
    _write_mosaic,
)
from ldn.tests.test_mosaic import _make_feature

# Shared fixture


BBOX = [100.0, 0.0, 101.0, 1.0]
BBOX2 = [102.0, 0.0, 103.0, 1.0]


# _stac_self_link


class TestStacSelfLink:
    def test_returns_self_href(self):
        feat = _make_feature("tile_1", BBOX)
        assert _stac_self_link(feat) == "https://example.com/items/tile_1"

    def test_raises_when_no_self_link(self):
        feat = _make_feature("tile_1", BBOX)
        feat["links"] = []
        with pytest.raises(Exception, match="no self link"):
            _stac_self_link(feat)

    def test_raises_includes_feature_id(self):
        feat = _make_feature("tile_99_88", BBOX)
        feat["links"] = []
        with pytest.raises(Exception, match="tile_99_88"):
            _stac_self_link(feat)

    def test_picks_self_among_multiple_links(self):
        feat = _make_feature("tile_1", BBOX)
        feat["links"] = [
            {"rel": "root", "href": "https://example.com/root"},
            {"rel": "self", "href": "https://example.com/self"},
            {"rel": "collection", "href": "https://example.com/collection"},
        ]
        assert _stac_self_link(feat) == "https://example.com/self"

    def test_raises_when_only_non_self_links_present(self):
        feat = _make_feature("tile_1", BBOX)
        feat["links"] = [{"rel": "root", "href": "https://example.com/root"}]
        with pytest.raises(Exception):
            _stac_self_link(feat)


# _extract_years


class TestExtractYears:
    def test_single_year(self):
        assert _extract_years([_make_feature("t1", BBOX, year="2020")]) == [2020]

    def test_multiple_years_sorted(self):
        features = [_make_feature(f"t{i}", BBOX, year=str(y)) for i, y in enumerate([2022, 2019, 2021])]
        assert _extract_years(features) == [2019, 2021, 2022]

    def test_deduplicates_years(self):
        features = [
            _make_feature("t1", BBOX, year="2020"),
            _make_feature("t2", BBOX2, year="2020"),
            _make_feature("t3", BBOX, year="2021"),
        ]
        assert _extract_years(features) == [2020, 2021]

    def test_empty_input(self):
        assert _extract_years([]) == []

    def test_skips_empty_datetime(self):
        feat = _make_feature("t1", BBOX, year="2020")
        feat["properties"]["datetime"] = ""
        assert _extract_years([feat]) == []

    def test_skips_missing_properties(self):
        feat = {"type": "Feature", "id": "x", "geometry": {}, "links": []}
        assert _extract_years([feat]) == []

    def test_parses_full_iso_string(self):
        feat = _make_feature("t1", BBOX, year="2018")
        feat["properties"]["datetime"] = "2018-12-31T23:59:59Z"
        assert _extract_years([feat]) == [2018]


# _build_mosaic_for_year


class TestBuildMosaicForYear:
    def test_returns_mosaic_json_instance(self):
        features = [_make_feature("t1", BBOX, year="2020"), _make_feature("t2", BBOX2, year="2020")]
        assert isinstance(_build_mosaic_for_year(2020, features), MosaicJSON)

    def test_raises_for_year_with_no_features(self):
        with pytest.raises(Exception, match="2099"):
            _build_mosaic_for_year(2099, [_make_feature("t1", BBOX, year="2020")])

    def test_filters_to_requested_year_only(self):
        features = [
            _make_feature("t1", BBOX, year="2020"),
            _make_feature("t2", BBOX2, year="2021"),
        ]
        assert _build_mosaic_for_year(2020, features) is not None

    def test_zoom_range(self):
        mosaic = _build_mosaic_for_year(2020, [_make_feature("t1", BBOX, year="2020")])
        assert mosaic.minzoom == 5
        assert mosaic.maxzoom == 12


# _find_stac_items_s3


class TestFindStacItemsS3:
    def _chunk(self, paths: list[str]) -> list[dict]:
        return [{"path": p} for p in paths]

    @patch("ldn.cli.obstore")
    def test_returns_matching_keys(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter(
            [self._chunk(["prefix/a.stac-item.json", "prefix/b.tif", "prefix/c.stac-item.json"])]
        )
        result = _find_stac_items_s3("my-bucket", "prefix/")
        assert result == ["prefix/a.stac-item.json", "prefix/c.stac-item.json"]

    @patch("ldn.cli.obstore")
    def test_empty_when_no_matches(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter([self._chunk(["prefix/a.tif", "prefix/b.parquet"])])
        assert _find_stac_items_s3("my-bucket", "prefix/") == []

    @patch("ldn.cli.obstore")
    def test_custom_suffix(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter([self._chunk(["a.parquet", "b.stac-item.json"])])
        assert _find_stac_items_s3("bucket", "prefix/", suffix=".parquet") == ["a.parquet"]

    @patch("ldn.cli.obstore")
    def test_public_flag_sets_skip_signature(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter([])
        _find_stac_items_s3("bucket", "prefix/", public=True)
        _, kwargs = mock_obstore.store.S3Store.call_args
        assert kwargs["skip_signature"] is True

    @patch("ldn.cli.obstore")
    def test_strips_leading_slash_from_prefix(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter([])
        _find_stac_items_s3("bucket", "/some/prefix")
        _, list_kwargs = mock_obstore.list.call_args
        assert not list_kwargs.get("prefix", "some/prefix").startswith("/")

    @patch("ldn.cli.obstore")
    def test_handles_multiple_chunks(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.list.return_value = iter(
            [
                self._chunk(["a.stac-item.json"]),
                self._chunk(["b.stac-item.json", "c.tif"]),
            ]
        )
        assert _find_stac_items_s3("bucket", "prefix/") == ["a.stac-item.json", "b.stac-item.json"]


# _load_stac_docs


class TestLoadStacDocs:
    def _fake_response(self, doc: dict) -> MagicMock:
        raw = MagicMock()
        raw.bytes.return_value = json.dumps(doc).encode()
        return raw

    @patch("ldn.cli.obstore")
    def test_returns_parsed_dicts(self, mock_obstore):
        docs = [_make_feature("t0", BBOX, year="2020"), _make_feature("t1", BBOX2, year="2021")]
        mock_obstore.store.S3Store.return_value = MagicMock()

        async def fake_get(store, key):
            return self._fake_response(docs[int(key)])

        mock_obstore.get_async = fake_get
        assert _load_stac_docs("bucket", ["0", "1"]) == docs

    @patch("ldn.cli.obstore")
    def test_preserves_order(self, mock_obstore):
        docs = [{"id": str(i)} for i in range(5)]
        mock_obstore.store.S3Store.return_value = MagicMock()

        async def fake_get(store, key):
            return self._fake_response(docs[int(key)])

        mock_obstore.get_async = fake_get
        result = _load_stac_docs("bucket", [str(i) for i in range(5)])
        assert [d["id"] for d in result] == [str(i) for i in range(5)]

    @patch("ldn.cli.obstore")
    def test_empty_keys_returns_empty(self, mock_obstore):
        mock_obstore.store.S3Store.return_value = MagicMock()
        mock_obstore.get_async = AsyncMock()
        assert _load_stac_docs("bucket", []) == []


# _write_mosaic


class TestWriteMosaic:
    def test_raises_for_non_s3_path(self):
        with pytest.raises(Exception, match="s3://"):
            _write_mosaic(MagicMock(), "/local/path/mosaic.json", MagicMock())

    def test_puts_to_correct_bucket_and_key(self):
        mosaic = MagicMock()
        mosaic.model_dump_json.return_value = '{"tiles": []}'
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        _write_mosaic(mosaic, "s3://my-bucket/path/to/mosaic.json", mock_session)

        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["Key"] == "path/to/mosaic.json"
        assert call_kwargs["ContentType"] == "application/json"

    def test_body_is_utf8_encoded_json(self):
        mosaic = MagicMock()
        mosaic.model_dump_json.return_value = '{"minzoom": 5}'
        mock_client = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_client

        _write_mosaic(mosaic, "s3://bucket/key.json", mock_session)

        body = mock_client.put_object.call_args.kwargs["Body"]
        assert isinstance(body, bytes)
        assert json.loads(body) == {"minzoom": 5}


# _run_index


class TestRunIndex:
    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.make_obstore_s3")
    @patch("ldn.cli.get_write_session")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_writes_combined_parquet(self, mock_find, mock_load, mock_session, mock_store, mock_write):
        features = [_make_feature("t1", BBOX, year="2020")]
        mock_find.return_value = ["key/a.stac-item.json"]
        mock_load.return_value = features

        _run_index("my-bucket", [("full/prefix", "short/prefix")], "output/index.parquet")

        mock_write.assert_called_once_with("output/index.parquet", features, store=mock_store.return_value)

    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.get_write_session")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_skips_write_when_no_items_found(self, mock_find, mock_load, mock_session, mock_write):
        mock_find.return_value = []
        _run_index("my-bucket", [("full/prefix", "short/prefix")], "output/index.parquet")
        mock_write.assert_not_called()

    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.make_obstore_s3")
    @patch("ldn.cli.get_write_session")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_combines_docs_across_multiple_targets(self, mock_find, mock_load, mock_session, mock_store, mock_write):
        mock_find.side_effect = [["key1.stac-item.json"], ["key2.stac-item.json"]]
        mock_load.side_effect = [
            [_make_feature("t1", BBOX, year="2020")],
            [_make_feature("t2", BBOX2, year="2021")],
        ]

        _run_index("bucket", [("prefix/a", "a"), ("prefix/b", "b")], "out.parquet")

        all_docs = mock_write.call_args[0][1]
        assert len(all_docs) == 2
