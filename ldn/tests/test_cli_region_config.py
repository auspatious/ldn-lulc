"""Smoke tests verifying CLI region config params wire through to S3ItemPath."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from ldn.cli import app
from ldn.utils import SOURCE_COOP_PREFIX_GEOMAD, SOURCE_COOP_PUBLIC_URL

runner = CliRunner()


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
                "--owner-pacific",
                "myorg",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_find_stac.assert_called_once()
        call_args = mock_find_stac.call_args

        assert call_args.args[0] == "my-custom-bucket"

        expected_prefix = "myorg_ls_geomad/"
        if SOURCE_COOP_PREFIX_GEOMAD:
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
        if SOURCE_COOP_PREFIX_GEOMAD:
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

    @patch("ldn.cli_geomad.object_exists", return_value=True)
    @patch("ldn.cli_geomad.configure_s3_access")
    @patch("ldn.cli_geomad.boto3")
    def test_custom_bucket_skips_existing(self, mock_boto3, mock_s3_access, mock_exists):
        """When item exists with custom bucket, geomad should skip and report the custom path."""
        mock_boto3.client.return_value = MagicMock()

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
                "0-2-1",
                "--region",
                "pacific",
                "--bucket",
                "my-test-bucket",
                "--owner-pacific",
                "testorg",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "already exists" in result.output
        mock_exists.assert_called_once()
        call_args = mock_exists.call_args
        assert call_args[0][0] == "my-test-bucket"
        assert "testorg_ls_geomad" in call_args[0][1]


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
                "--owner-pacific",
                "idxorg",
            ],
        )

        assert result.exit_code == 0, result.output
        if SOURCE_COOP_PUBLIC_URL:
            mock_run_index.assert_called_once_with(
                "idx-bucket",
                [("auspatious/geomad-sids/idxorg_ls_geomad/0-2-1", "idxorg_ls_geomad")],
                "auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet",
            )
        else:
            mock_run_index.assert_called_once_with(
                "idx-bucket",
                [("idxorg_ls_geomad/0-2-1", "idxorg_ls_geomad")],
                "None/ls_geomad/0-2-1/ls_geomad.parquet",
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
        if SOURCE_COOP_PUBLIC_URL:
            assert args[1] == [("auspatious/geomad-sids/custom_ls_geomad/0-2-1", "custom_ls_geomad")]
        else:
            assert args[1] == [("custom_ls_geomad/0-2-1", "custom_ls_geomad")]
