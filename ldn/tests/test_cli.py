from unittest.mock import MagicMock, patch

import pytest
from cogeo_mosaic.mosaic import MosaicJSON

from ldn.cli import (
    _build_mosaic_for_year,
    _extract_years,
    _find_stac_items_s3,
    _load_stac_docs,
    _stac_self_link,
    _write_mosaic,
    index_to_stac_geoparquet,
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
    @patch("ldn.cli.s3_client")
    def test_returns_matching_keys(self, mock_s3_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "prefix/a.stac-item.json"},
                    {"Key": "prefix/b.tif"},
                    {"Key": "prefix/c.stac-item.json"},
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = paginator
        result = _find_stac_items_s3("my-bucket", "prefix/")
        assert result == ["prefix/a.stac-item.json", "prefix/c.stac-item.json"]

    @patch("ldn.cli.s3_client")
    def test_empty_when_no_matches(self, mock_s3_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "prefix/a.tif"}, {"Key": "prefix/b.parquet"}]}]
        mock_s3_client.get_paginator.return_value = paginator
        assert _find_stac_items_s3("my-bucket", "prefix/") == []

    @patch("ldn.cli.s3_client")
    def test_custom_suffix(self, mock_s3_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "a.parquet"}, {"Key": "b.stac-item.json"}]}]
        mock_s3_client.get_paginator.return_value = paginator
        assert _find_stac_items_s3("bucket", "prefix/", suffix=".parquet") == ["a.parquet"]

    @patch("ldn.cli.s3_client")
    def test_handles_multiple_pages(self, mock_s3_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "a.stac-item.json"}]},
            {"Contents": [{"Key": "b.stac-item.json"}, {"Key": "c.tif"}]},
        ]
        mock_s3_client.get_paginator.return_value = paginator
        assert _find_stac_items_s3("bucket", "prefix/") == ["a.stac-item.json", "b.stac-item.json"]


# _load_stac_docs


class TestLoadStacDocs:
    @patch("ldn.cli._load_stac_docs_async")
    @patch("ldn.cli.asyncio.run")
    def test_returns_parsed_dicts(self, mock_run, mock_load_async):
        docs = [_make_feature("t0", BBOX, year="2020"), _make_feature("t1", BBOX2, year="2021")]
        mock_load_async.return_value = docs
        mock_run.return_value = docs
        assert _load_stac_docs("bucket", ["0", "1"]) == docs
        mock_run.assert_called_once()

    @patch("ldn.cli._load_stac_docs_async")
    @patch("ldn.cli.asyncio.run")
    def test_preserves_order(self, mock_run, mock_load_async):
        docs = [{"id": str(i)} for i in range(5)]
        mock_load_async.return_value = docs
        mock_run.return_value = docs
        result = _load_stac_docs("bucket", [str(i) for i in range(5)])
        assert [d["id"] for d in result] == [str(i) for i in range(5)]

    @patch("ldn.cli._load_stac_docs_async")
    @patch("ldn.cli.asyncio.run")
    def test_empty_keys_returns_empty(self, mock_run, mock_load_async):
        mock_load_async.return_value = []
        mock_run.return_value = []
        assert _load_stac_docs("bucket", []) == []


# _write_mosaic


class TestWriteMosaic:
    @patch("ldn.cli.s3_client")
    def test_puts_to_correct_bucket_and_key(self, mock_s3_client):
        mosaic = MagicMock()
        mosaic.model_dump_json.return_value = '{"tiles": []}'

        _write_mosaic(mosaic, "my-bucket", "path/to/mosaic.json")

        call_kwargs = mock_s3_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["Key"] == "path/to/mosaic.json"
        assert call_kwargs["ContentType"] == "application/json"

    @patch("ldn.cli.s3_client")
    def test_body_is_utf8_encoded_json(self, mock_s3_client):
        mosaic = MagicMock()
        mosaic.model_dump_json.return_value = '{"minzoom": 5}'

        _write_mosaic(mosaic, "bucket", "key.json")

        body = mock_s3_client.put_object.call_args.kwargs["Body"]
        assert isinstance(body, bytes)
        assert body.decode("utf-8") == '{"minzoom": 5}'


class TestIndexToStacGeoparquet:
    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.Boto3CredentialProvider")
    @patch("ldn.cli.obstore.store.S3Store")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_writes_combined_parquet(self, mock_find, mock_load, mock_store, mock_credential_provider, mock_write):
        mock_find.side_effect = [["key1.stac-item.json"], ["key2.stac-item.json"]]
        mock_load.side_effect = [
            [_make_feature("t1", BBOX, year="2020")],
            [_make_feature("t2", BBOX2, year="2021")],
        ]

        index_to_stac_geoparquet(
            dataset="geomad",
            region="all",
            version_geomad="0-0-1",
            version_lulc="0-0-1",
            bucket="dep-public-staging",
            product_owner=None,
        )

        assert mock_write.call_count == 1
        output_key = mock_write.call_args[0][0]
        all_docs = mock_write.call_args[0][1]
        assert output_key == "ls_geomad/0-0-1/ls_geomad.parquet"
        assert len(all_docs) == 2
        assert mock_write.call_args.kwargs["store"] == mock_store.return_value

    @patch("ldn.cli.write_sync")
    @patch("ldn.cli._find_stac_items_s3")
    def test_skips_when_no_items_found(self, mock_find, mock_write):
        mock_find.return_value = []

        index_to_stac_geoparquet(
            dataset="geomad",
            region="all",
            version_geomad="0-0-1",
            version_lulc="0-0-1",
            bucket="dep-public-staging",
            product_owner=None,
        )

        mock_write.assert_not_called()
