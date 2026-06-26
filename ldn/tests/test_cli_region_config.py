"""Smoke tests verifying CLI region config params wire through to S3ItemPath."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ldn.cli import app
from ldn.utils import (
    GEOMAD_VERSION,
    SOURCE_COOP_PREFIX_GEOMAD,
    get_env_var,
    is_bucket_source_coop,
)

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

        expected_prefix = "dep_ls_geomad/"
        _is_bucket_source_coop = is_bucket_source_coop(self.BUCKET)
        if _is_bucket_source_coop:
            expected_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{expected_prefix}"
        assert call_args.args[1].startswith(expected_prefix)

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
        expected_prefix = "override_ls_geomad/"
        _is_bucket_source_coop = is_bucket_source_coop(get_env_var("BUCKET"))
        if _is_bucket_source_coop:
            expected_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{expected_prefix}"
        assert call_args.args[1].startswith(expected_prefix)

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
        assert "lulc" in call_args.args[1]


class TestGeomadRegionConfig:
    """Verify geomad command wires bucket/owner into S3ItemPath."""

    @patch("ldn.cli_geomad._count_scenes", return_value=25)
    @patch("ldn.cli_geomad.configure_s3_access_profile")
    @patch("ldn.cli_geomad.get_gridspec")
    @patch("ldn.cli_geomad.build_pipeline_components", return_value=None)
    def test_custom_bucket_skips_existing(self, mock_build, mock_get_gridspec, mock_s3_access_profile, mock_count):
        """Custom bucket/owner should be forwarded when building GeoMAD pipeline components."""
        mock_get_gridspec.return_value.tile_geobox.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "geomad",
                "run",
                "--tile-id",
                "066_022",
                "--year",
                "2020",
                "--version",
                GEOMAD_VERSION,
                "--region",
                "pacific",
                "--bucket",
                "my-test-bucket",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_build.assert_called_once()
        call_args = mock_build.call_args.args
        assert call_args[3] == "my-test-bucket"
        assert call_args[5] == "geomad"


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
        _is_bucket_source_coop = is_bucket_source_coop(self.BUCKET)
        expected_prefix = f"custom_ls_geomad/{GEOMAD_VERSION}"
        if _is_bucket_source_coop:
            expected_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{expected_prefix}"
        mock_find.assert_called_once_with(self.BUCKET, expected_prefix)
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
        _is_bucket_source_coop = is_bucket_source_coop(get_env_var("BUCKET"))
        expected_prefix = f"custom_ls_geomad/{GEOMAD_VERSION}"
        if _is_bucket_source_coop:
            expected_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{expected_prefix}"
        mock_find.assert_called_once_with(get_env_var("BUCKET"), expected_prefix)
        mock_write.assert_called_once()
        assert mock_write.call_args[0][0] == f"custom_ls_geomad/{GEOMAD_VERSION}/custom_ls_geomad.parquet"
