"""Smoke tests verifying CLI region config params wire through to S3ItemPath."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from ldn.cli import app
from ldn.utils import GEOMAD_VERSION

runner = CliRunner()


@pytest.fixture(autouse=True)
def mock_required_env(monkeypatch):
    """Set required CLI env vars so tests do not depend on shell state."""
    monkeypatch.setenv("BUCKET", "dep-public-staging")


class TestPrintTasksRegionConfig:
    """Verify print_tasks passes bucket/owner params through to S3ItemPath."""

    BUCKET = "my-custom-bucket"

    @patch("ldn.cli.get_grid_tiles")
    @patch("ldn.cli._find_stac_items_s3")
    def test_custom_bucket_and_owner_used_in_listing(self, mock_find_stac, mock_get_tiles):
        """Custom bucket/owner should appear in the S3 listing call."""
        mock_get_tiles.return_value = [((66, 22), "pacific")]
        mock_find_stac.return_value = []

        result = runner.invoke(
            app,
            [
                "print-tasks",
                "--years",
                "2020",
                "--region",
                "pacific",
                "--bucket",
                self.BUCKET,
            ],
        )

        assert result.exit_code == 0, result.output
        mock_find_stac.assert_called_once()
        call_args = mock_find_stac.call_args

        assert call_args.args[0] == self.BUCKET
        assert call_args.args[1] == f"dep_ls_geomad/{GEOMAD_VERSION}/"

    @patch("ldn.cli.get_grid_tiles")
    @patch("ldn.cli._find_stac_items_s3")
    def test_product_owner_overrides_region_default(self, mock_find_stac, mock_get_tiles):
        """--product-owner should override the region-derived owner."""
        mock_get_tiles.return_value = [((119, 126), "non-pacific")]
        mock_find_stac.return_value = []

        result = runner.invoke(
            app,
            [
                "print-tasks",
                "--years",
                "2020",
                "--region",
                "non-pacific",
                "--product-owner",
                "override",
            ],
        )

        assert result.exit_code == 0, result.output
        call_args = mock_find_stac.call_args
        assert call_args.args[0] == "dep-public-staging"
        assert call_args.args[1] == f"override_ls_geomad/{GEOMAD_VERSION}/"

    @patch("ldn.cli.get_grid_tiles")
    @patch("ldn.cli._find_stac_items_s3")
    def test_filter_tasks_lulc_dataset(self, mock_find_stac, mock_get_tiles):
        """--dataset lulc should use lulc prefix."""
        mock_get_tiles.return_value = [((66, 22), "pacific")]
        mock_find_stac.return_value = []

        result = runner.invoke(
            app,
            [
                "print-tasks",
                "--years",
                "2020",
                "--region",
                "pacific",
                "--dataset",
                "lulc",
            ],
        )

        assert result.exit_code == 0, result.output
        call_args = mock_find_stac.call_args
        assert call_args.args[0] == "dep-public-staging"
        assert call_args.args[1].startswith("dep_ls_lulc/")
        assert call_args.args[1].endswith("/")


class TestIndexToStacGeoparquetRegionConfig:
    """Verify index-to-stac-geoparquet wires bucket/owner params correctly."""

    BUCKET = "idx-bucket"

    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.rustac.store.S3Store")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_custom_bucket_and_owner(
        self,
        mock_find,
        mock_load,
        mock_store,
        mock_write,
    ):
        """Custom bucket/owner should be used in listing and write target."""
        mock_find.return_value = ["a.stac-item.json"]
        mock_load.return_value = [{"id": "item-1"}]

        result = runner.invoke(
            app,
            [
                "index-to-stac-geoparquet",
                "--dataset",
                "geomad",
                "--single-region",
                "--product-owner",
                "custom",
                "--bucket",
                self.BUCKET,
            ],
        )

        assert result.exit_code == 0, result.output
        mock_find.assert_called_once()
        find_bucket, find_prefix = mock_find.call_args.args
        assert find_bucket == self.BUCKET
        assert find_prefix == f"custom_ls_geomad/{GEOMAD_VERSION}"
        mock_write.assert_called_once()
        assert mock_write.call_args[0][0] == f"custom_ls_geomad/{GEOMAD_VERSION}/custom_ls_geomad.parquet"

    @patch("ldn.cli.write_sync")
    @patch("ldn.cli.rustac.store.S3Store")
    @patch("ldn.cli._load_stac_docs")
    @patch("ldn.cli._find_stac_items_s3")
    def test_product_owner_override(
        self,
        mock_find,
        mock_load,
        mock_store,
        mock_write,
    ):
        """--product-owner overrides the region-derived owner."""
        mock_find.return_value = ["a.stac-item.json"]
        mock_load.return_value = [{"id": "item-1"}]

        result = runner.invoke(
            app,
            [
                "index-to-stac-geoparquet",
                "--dataset",
                "geomad",
                "--single-region",
                "--product-owner",
                "custom",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_find.assert_called_once()
        find_bucket, find_prefix = mock_find.call_args.args
        assert find_bucket == "dep-public-staging"
        assert find_prefix == f"custom_ls_geomad/{GEOMAD_VERSION}"
        mock_write.assert_called_once()
        assert mock_write.call_args[0][0] == f"custom_ls_geomad/{GEOMAD_VERSION}/custom_ls_geomad.parquet"
