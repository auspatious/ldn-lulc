import importlib
import os

import boto3
import pytest
from click.testing import CliRunner

import ldn.utils
from ldn.cli_geomad import run
from ldn.raster import PrefixedS3ItemPath
from ldn.utils import GEOMAD_DATASET_ID, SENSOR, get_full_path_prefix, parse_tile_id

pytestmark = pytest.mark.integration


INTEGRATION_CONFIGS = [
    {
        "id": "auspatious",
        "BUCKET": "data.ldn.auspatious.com",
        "SOURCE_COOP_PUBLIC_URL": "",
        "SOURCE_COOP_PREFIX_GEOMAD": "",
        "SOURCE_COOP_PREFIX_LULC": "",
    },
    {
        "id": "dep-staging",
        "BUCKET": "dep-public-staging",
        "SOURCE_COOP_PUBLIC_URL": "",
        "SOURCE_COOP_PREFIX_GEOMAD": "",
        "SOURCE_COOP_PREFIX_LULC": "",
    },
    # Source.Coop requires AWS_WRITE_* env vars — skip if not set
    {
        "id": "source-coop",
        "BUCKET": "us-west-2.opendata.source.coop",
        "SOURCE_COOP_PUBLIC_URL": "https://data.source.coop",
        "SOURCE_COOP_PREFIX_GEOMAD": "auspatious/geomad-sids",
        "SOURCE_COOP_PREFIX_LULC": "auspatious/lulc-sids",
    },
]


@pytest.fixture(params=INTEGRATION_CONFIGS, ids=[c["id"] for c in INTEGRATION_CONFIGS])
def bucket_env(request, monkeypatch):
    config = request.param
    if config["id"] == "source-coop" and not os.environ.get("AWS_WRITE_ACCESS_KEY_ID"):
        pytest.skip("Source.Coop write credentials not set")
    for k, v in config.items():
        if k != "id":
            monkeypatch.setenv(k, v)
    importlib.reload(ldn.utils)
    yield config
    importlib.reload(ldn.utils)  # restore after test


# TODO: Add an AM-Crossing integration test. 066_022.
TILE_ID = "028_030"
YEAR = "2025"
VERSION = "integration-test"


@pytest.fixture
def runner():
    return CliRunner()


def test_geomad_run_decimated(bucket_env, runner):
    """Full pipeline run on a small decimated tile - checks exit 0 and writes STAC item."""
    result = runner.invoke(
        run,
        [
            "--tile-id",
            TILE_ID,
            "--year",
            YEAR,
            "--version",
            VERSION,
            "--region",
            "pacific",
            "--decimated",
            "--overwrite",
        ],
    )
    assert result.exit_code == 0, result.output


def test_geomad_skips_existing(bucket_env, runner):
    """Second run without overwrite should skip cleanly."""
    # First run to ensure item exists
    runner.invoke(
        run,
        [
            "--tile-id",
            TILE_ID,
            "--year",
            YEAR,
            "--version",
            VERSION,
            "--region",
            "pacific",
            "--decimated",
            "--overwrite",
        ],
    )

    # Second run should skip
    result = runner.invoke(
        run,
        [
            "--tile-id",
            TILE_ID,
            "--year",
            YEAR,
            "--version",
            VERSION,
            "--region",
            "pacific",
            "--decimated",
        ],
    )
    assert result.exit_code == 0
    assert "skipping" in result.output.lower()


def test_geomad_stac_item_exists_in_bucket(bucket_env):
    """After a run, the STAC item should be readable from S3."""
    bucket = bucket_env["BUCKET"]
    source_coop_prefix = bucket_env.get("SOURCE_COOP_PREFIX_GEOMAD") or None
    tile_id_tuple = parse_tile_id(TILE_ID)

    itempath = PrefixedS3ItemPath(
        key_prefix=source_coop_prefix,
        prefix="dep",
        bucket=bucket,
        sensor=SENSOR,
        dataset_id=GEOMAD_DATASET_ID,
        version=VERSION,
        time=YEAR,
        full_path_prefix=get_full_path_prefix(bucket),
    )
    stac_key = itempath.stac_path(tile_id_tuple, absolute=False)
    s3 = boto3.client("s3")
    response = s3.head_object(Bucket=bucket, Key=stac_key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
