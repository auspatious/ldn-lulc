import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from cogeo_mosaic.mosaic import MosaicJSON
from typer.testing import CliRunner

from ldn.cli import (
    _build_mosaic_for_year,
    _extract_years,
    _find_stac_items_s3,
    _load_stac_docs,
    _stac_self_link,
    app,
    index_to_stac_geoparquet,
)
from ldn.tests.test_mosaic import _make_feature, _make_item
from ldn.utils import LdnError

# Shared fixture


BBOX = [100.0, 0.0, 101.0, 1.0]
BBOX2 = [102.0, 0.0, 103.0, 1.0]
runner = CliRunner()


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


def test_cli_help_works_without_aws_credentials():
    """The CLI should import and render help without ambient AWS credentials."""
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0, result.output


def test_credential_provider_raises_when_used_without_aws_credentials(monkeypatch):
    """get_credential_provider() should still fail when actually invoked with no
    AWS credentials present - confirming the fix only defers the check, it doesn't
    silently swallow missing credentials at the point they're actually needed."""
    from ldn import aws as aws_module

    monkeypatch.setattr(aws_module.aws_session, "get_credentials", lambda: None)
    aws_module.get_credential_provider.cache_clear()

    with pytest.raises(ValueError, match="Received None from session.get_credentials"):
        aws_module.get_credential_provider()


# _extract_years


class TestExtractYears:
    def test_single_year(self):
        assert _extract_years([_make_item("t1", BBOX, year="2020")]) == [2020]

    def test_multiple_years_sorted(self):
        features = [_make_item(f"t{i}", BBOX, year=str(y)) for i, y in enumerate([2022, 2019, 2021])]
        assert _extract_years(features) == [2019, 2021, 2022]

    def test_deduplicates_years(self):
        features = [
            _make_item("t1", BBOX, year="2020"),
            _make_item("t2", BBOX2, year="2020"),
            _make_item("t3", BBOX, year="2021"),
        ]
        assert _extract_years(features) == [2020, 2021]

    def test_empty_input(self):
        assert _extract_years([]) == []

    def test_skips_empty_datetime(self):
        feat = SimpleNamespace(datetime=None)
        assert _extract_years([feat]) == []

    def test_skips_missing_properties(self):
        feat = SimpleNamespace(datetime=None)
        assert _extract_years([feat]) == []

    def test_parses_full_iso_string(self):
        feat = _make_item("t1", BBOX, year="2018")
        assert _extract_years([feat]) == [2018]


# _build_mosaic_for_year


class TestBuildMosaicForYear:
    def test_returns_mosaic_json_instance(self):
        features = [_make_item("t1", BBOX, year="2020"), _make_item("t2", BBOX2, year="2020")]
        assert isinstance(_build_mosaic_for_year(2020, features), MosaicJSON)

    def test_raises_for_year_with_no_features(self):
        with pytest.raises(Exception, match="2099"):
            _build_mosaic_for_year(2099, [_make_item("t1", BBOX, year="2020")])

    def test_filters_to_requested_year_only(self):
        features = [
            _make_item("t1", BBOX, year="2020"),
            _make_item("t2", BBOX2, year="2021"),
        ]
        mosaic = _build_mosaic_for_year(2020, features)
        tile_hrefs = [href for hrefs in mosaic.tiles.values() for href in hrefs]
        assert all("/t1" in href for href in tile_hrefs)
        assert all("/t2" not in href for href in tile_hrefs)

    def test_zoom_range(self):
        mosaic = _build_mosaic_for_year(2020, [_make_item("t1", BBOX, year="2020")])
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
    def test_filters_stac_suffix_only(self, mock_s3_client):
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "a.parquet"}, {"Key": "b.stac-item.json"}]}]
        mock_s3_client.get_paginator.return_value = paginator
        assert _find_stac_items_s3("bucket", "prefix/") == ["b.stac-item.json"]

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
    @patch("ldn.cli.s3_client.get_object")
    def test_returns_parsed_dicts(self, mock_get_object):
        docs = [_make_feature("t0", BBOX, year="2020"), _make_feature("t1", BBOX2, year="2021")]

        payloads = {
            "0": docs[0],
            "1": docs[1],
        }

        def _fake_get_object(Bucket, Key):
            return {"Body": io.BytesIO(json.dumps(payloads[Key]).encode("utf-8"))}

        mock_get_object.side_effect = _fake_get_object
        result = _load_stac_docs("bucket", ["0", "1"])

        assert result == docs
        assert mock_get_object.call_count == 2

    @patch("ldn.cli.s3_client.get_object")
    def test_preserves_order(self, mock_get_object):
        def _fake_get_object(Bucket, Key):
            return {"Body": io.BytesIO(json.dumps({"id": Key}).encode("utf-8"))}

        mock_get_object.side_effect = _fake_get_object
        keys = [str(i) for i in range(5)]
        result = _load_stac_docs("bucket", keys)
        assert [d["id"] for d in result] == [str(i) for i in range(5)]

    @patch("ldn.cli.s3_client.get_object")
    def test_empty_keys_returns_empty(self, mock_get_object):
        assert _load_stac_docs("bucket", []) == []
        mock_get_object.assert_not_called()


class TestIndexToStacGeoparquet:
    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.rustac.store.S3Store")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_writes_combined_parquet(self, mock_find, mock_load, mock_store, mock_write):
        mock_find.side_effect = [["key1.stac-item.json"], ["key2.stac-item.json"]]
        mock_load.side_effect = [
            [_make_feature("t1", BBOX, year="2020")],
            [_make_feature("t2", BBOX2, year="2021")],
        ]

        index_to_stac_geoparquet(
            dataset="geomad",
            geomad_version="0-0-1",
            lulc_version="0-0-1",
            bucket="dep-public-staging",
            product_owner=None,
            single_region=False,
            sensor="ls",
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
        mock_find.side_effect = [[], []]

        with pytest.raises(LdnError, match="No STAC items found"):
            index_to_stac_geoparquet(
                dataset="geomad",
                geomad_version="0-0-1",
                lulc_version="0-0-1",
                bucket="dep-public-staging",
                product_owner=None,
                single_region=False,
                sensor="ls",
            )

        mock_write.assert_not_called()
