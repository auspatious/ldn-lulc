# Seperate folder for integration tests so they can be easily excluded from regular test runs.
import importlib
import os
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from typer.testing import CliRunner

import ldn.utils
from ldn.cli_geomad import geomad_app
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


@pytest.fixture
def stac_key(bucket_env):
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
    return itempath.stac_path(tile_id_tuple, absolute=False)


def test_geomad_run_and_skip(bucket_env, runner, stac_key):
    """Write with overwrite, check item was recently written, then check skip doesn't overwrite."""
    bucket = bucket_env["BUCKET"]
    s3 = boto3.client("s3")

    # 1. Write with overwrite
    result = runner.invoke(
        geomad_app,
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

    # 2. Check item exists and was written in the last 15 minutes
    response = s3.head_object(Bucket=bucket, Key=stac_key)
    last_modified = response["LastModified"]
    assert datetime.now(UTC) - last_modified < timedelta(minutes=15), (
        f"STAC item was not recently written: {last_modified}"
    )

    # 3. Call without overwrite and check item wasn't rewritten
    result = runner.invoke(
        geomad_app,
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
    assert result.exit_code == 0, result.output

    after = s3.head_object(Bucket=bucket, Key=stac_key)["LastModified"]
    assert last_modified == after, (
        f"Item was rewritten when it should have been skipped. Before: {last_modified}, After: {after}"
    )
