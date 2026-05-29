"""Smoke tests verifying CLI region config params wire through to S3ItemPath."""

import json
from unittest.mock import patch, MagicMock

from typer.testing import CliRunner

from ldn.cli import app
from ldn.cli_classify import classify_app

runner = CliRunner()


class TestFilterTasksRegionConfig:
    """Verify filter_tasks passes bucket/owner params through to S3ItemPath."""

    @patch("ldn.cli.boto3")
    def test_custom_bucket_and_owner_used_in_listing(self, mock_boto3):
        """Custom bucket/owner should appear in the S3 paginator call."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        tasks = [{"id": "066_022", "year": "2020", "region": "pacific"}]

        result = runner.invoke(
            app,
            [
                "filter-tasks",
                "--tasks-json",
                json.dumps(tasks),
                "--version",
                "0-2-1",
                "--bucket-pacific",
                "my-custom-bucket",
                "--owner-pacific",
                "myorg",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_paginator.paginate.assert_called_once()
        call_kwargs = mock_paginator.paginate.call_args[1]
        assert call_kwargs["Bucket"] == "my-custom-bucket"
        assert call_kwargs["Prefix"].startswith("myorg_ls_geomad/")

    @patch("ldn.cli.boto3")
    def test_product_owner_overrides_region_default(self, mock_boto3):
        """--product-owner should override the region-derived owner."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        tasks = [{"id": "119_126", "year": "2020", "region": "non-pacific"}]

        result = runner.invoke(
            app,
            [
                "filter-tasks",
                "--tasks-json",
                json.dumps(tasks),
                "--version",
                "0-2-1",
                "--product-owner",
                "override",
            ],
        )

        assert result.exit_code == 0, result.output
        call_kwargs = mock_paginator.paginate.call_args[1]
        assert call_kwargs["Prefix"].startswith("override_ls_geomad/")

    @patch("ldn.cli.boto3")
    def test_filter_tasks_prediction_dataset(self, mock_boto3):
        """--dataset prediction should use lulc_prediction prefix."""
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        tasks = [{"id": "066_022", "year": "2020", "region": "pacific"}]

        result = runner.invoke(
            app,
            [
                "filter-tasks",
                "--tasks-json",
                json.dumps(tasks),
                "--version",
                "0-0-4",
                "--dataset",
                "prediction",
            ],
        )

        assert result.exit_code == 0, result.output
        call_kwargs = mock_paginator.paginate.call_args[1]
        assert "lulc_prediction" in call_kwargs["Prefix"]


class TestGeomadRegionConfig:
    """Verify geomad command wires bucket/owner into S3ItemPath."""

    @patch("ldn.cli.object_exists", return_value=True)
    @patch("ldn.cli.configure_s3_access")
    @patch("ldn.cli.boto3")
    def test_custom_bucket_skips_existing(
        self, mock_boto3, mock_s3_access, mock_exists
    ):
        """When item exists with custom bucket, geomad should skip and report the custom path."""
        mock_boto3.client.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "geomad",
                "--tile-id",
                "066_022",
                "--year",
                "2020",
                "--version",
                "0-2-1",
                "--region",
                "pacific",
                "--bucket-pacific",
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


class TestClassifyRegionConfig:
    """Verify classify command wires bucket/owner params to run_classify_task."""

    @patch("ldn.cli_classify.run_classify_task")
    def test_custom_bucket_and_owner_passed_to_classify(self, mock_run):
        """Custom bucket/owner should be forwarded to run_classify_task."""
        mock_run.return_value = None

        result = runner.invoke(
            classify_app,
            [
                "classify",
                "--tile-id",
                "066_022",
                "--year",
                "2020",
                "--version",
                "0-0-4",
                "--region",
                "pacific",
                "--bucket-pacific",
                "custom-bucket",
                "--owner-pacific",
                "customorg",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_run.assert_called_once()
        kwargs = mock_run.call_args[1]
        assert kwargs["output_bucket"] == "custom-bucket"
        assert kwargs["output_prefix"] == "customorg"
        assert kwargs["geomad_prefix"] == "customorg_ls_geomad"

    @patch("ldn.cli_classify.run_classify_task")
    def test_product_owner_overrides_in_classify(self, mock_run):
        """--product-owner should override the region-derived owner."""
        mock_run.return_value = None

        result = runner.invoke(
            classify_app,
            [
                "classify",
                "--tile-id",
                "119_126",
                "--year",
                "2020",
                "--version",
                "0-0-4",
                "--region",
                "non-pacific",
                "--product-owner",
                "override",
            ],
        )

        assert result.exit_code == 0, result.output
        kwargs = mock_run.call_args[1]
        assert kwargs["output_prefix"] == "override"
        assert kwargs["geomad_prefix"] == "override_ls_geomad"


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
                "--bucket-pacific",
                "idx-bucket",
                "--owner-pacific",
                "idxorg",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_run_index.assert_called_once_with(
            "idx-bucket", "idxorg_ls_geomad", "0-2-1"
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
        assert args[1] == "custom_ls_geomad"


class TestMakeMosaicsRegionConfig:
    """Verify make-mosaics wires bucket/owner params into parquet URL and output path."""

    @patch("ldn.cli._load_all_features", return_value=[])
    def test_custom_bucket_and_owner_in_parquet_url(self, mock_load):
        """Custom bucket/owner should appear in the stac-geoparquet URL."""
        result = runner.invoke(
            app,
            [
                "make-mosaics",
                "--dataset",
                "geomad",
                "--region",
                "pacific",
                "--bucket-pacific",
                "mosaic-bucket",
                "--owner-pacific",
                "mosaicorg",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_load.assert_called_once()
        url = mock_load.call_args[0][0]
        assert "mosaic-bucket" in url
        assert "mosaicorg_ls_geomad" in url

    @patch("ldn.cli._load_all_features", return_value=[])
    def test_product_owner_override_in_mosaic(self, mock_load):
        """--product-owner should override the region-derived owner."""
        result = runner.invoke(
            app,
            [
                "make-mosaics",
                "--dataset",
                "prediction",
                "--region",
                "non-pacific",
                "--product-owner",
                "ovr",
            ],
        )

        assert result.exit_code == 0, result.output
        mock_load.assert_called_once()
        url = mock_load.call_args[0][0]
        assert "ovr_ls_lulc_prediction" in url
