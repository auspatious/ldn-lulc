"""Smoke tests verifying CLI region config params wire through to S3ItemPath."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ldn.cli import app
from ldn.utils import GEOMAD_VERSION, SOURCE_COOP_PREFIX_GEOMAD, get_bool_env_var, get_stac_geoparquet_key

runner = CliRunner()


@pytest.fixture(autouse=True)
def mock_required_env(monkeypatch):
    """Set required CLI env vars so tests do not depend on shell state."""
    monkeypatch.setenv("BUCKET", "dep-public-staging")
    monkeypatch.setenv("IS_SOURCE_COOP", "false")


class TestPrintTasksRegionConfig:
    """Verify print_tasks passes bucket/owner params through to S3ItemPath."""

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
                "my-custom-bucket",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_find_stac.assert_called_once()
        call_args = mock_find_stac.call_args

        assert call_args.args[0] == "my-custom-bucket"

        expected_prefix = "dep_ls_geomad/"
        _is_source_coop = get_bool_env_var("IS_SOURCE_COOP")
        if _is_source_coop:
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
        _is_source_coop = get_bool_env_var("IS_SOURCE_COOP")
        if _is_source_coop:
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
    @patch("ldn.cli_geomad.configure_s3_access")
    @patch("ldn.cli_geomad.get_gridspec")
    @patch("ldn.cli_geomad.build_pipeline_components", return_value=None)
    def test_custom_bucket_skips_existing(self, mock_build, mock_get_gridspec, mock_s3_access, mock_count):
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

    @patch("ldn.cli._run_index")
    def test_custom_bucket_and_owner(self, mock_run_index):
        """Custom bucket/owner should be forwarded to _run_index."""
        result = runner.invoke(
            app,
            [
                "index-to-stac-geoparquet",
                "--dataset",
                "geomad",
                "--region",
                "pacific",
                "--bucket",
                "idx-bucket",
            ],
        )

        assert result.exit_code == 0, result.output
        _is_source_coop = get_bool_env_var("IS_SOURCE_COOP")
        expected_parquet_key = get_stac_geoparquet_key("geomad", GEOMAD_VERSION, SOURCE_COOP_PREFIX_GEOMAD)
        if _is_source_coop:
            mock_run_index.assert_called_once_with(
                "idx-bucket",
                [(f"{SOURCE_COOP_PREFIX_GEOMAD}/dep_ls_geomad/{GEOMAD_VERSION}", "dep_ls_geomad")],
                expected_parquet_key,
            )
        else:
            mock_run_index.assert_called_once_with(
                "idx-bucket",
                [(f"dep_ls_geomad/{GEOMAD_VERSION}", "dep_ls_geomad")],
                expected_parquet_key,
            )

    @patch("ldn.cli._run_index")
    def test_product_owner_override(self, mock_run_index):
        """--product-owner overrides the region-derived owner."""
        result = runner.invoke(
            app,
            [
                "index-to-stac-geoparquet",
                "--dataset",
                "geomad",
                "--region",
                "non-pacific",
                "--product-owner",
                "custom",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_run_index.assert_called_once()
        args = mock_run_index.call_args[0]
        _is_source_coop = get_bool_env_var("IS_SOURCE_COOP")
        if _is_source_coop:
            assert args[1] == [(f"{SOURCE_COOP_PREFIX_GEOMAD}/custom_ls_geomad/{GEOMAD_VERSION}", "custom_ls_geomad")]
        else:
            assert args[1] == [(f"custom_ls_geomad/{GEOMAD_VERSION}", "custom_ls_geomad")]
