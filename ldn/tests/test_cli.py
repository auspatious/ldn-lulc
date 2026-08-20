import io
import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ldn.cli import (
    _find_stac_items_s3,
    _load_stac_docs,
    app,
    index_to_stac_geoparquet,
)
from ldn.utils import LdnError

# Shared fixture


BBOX = [100.0, 0.0, 101.0, 1.0]
BBOX2 = [102.0, 0.0, 103.0, 1.0]
runner = CliRunner()


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


def _make_feature(item_id: str, bbox: list[float], year: str = "2020") -> dict:
    """Helper to create a STAC feature dict."""
    minx, miny, maxx, maxy = bbox
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [minx, miny],
                    [maxx, miny],
                    [maxx, maxy],
                    [minx, maxy],
                    [minx, miny],
                ]
            ],
        },
        "links": [{"rel": "self", "href": f"https://example.com/items/{item_id}"}],
        "properties": {"datetime": f"{year}-06-01T00:00:00Z"},
        "assets": {},
    }


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
    @patch("ldn.cli.get_credential_provider", return_value=None)
    @patch("ldn.cli.rustac.store.S3Store")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_writes_combined_parquet(self, mock_find, mock_load, mock_store, mock_get_cred, mock_write):
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
