# Seperate folder for integration tests so they can be easily excluded from regular test runs.
import os
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from typer.testing import CliRunner

from ldn.aws_credentials import get_write_session
from ldn.cli_geomad import geomad_app
from ldn.raster import PrefixedS3ItemPath
from ldn.utils import GEOMAD_DATASET_ID, SENSOR, get_source_coop_config, parse_tile_id

pytestmark = pytest.mark.integration


INTEGRATION_CONFIGS = [
    {
        "id": "auspatious",
        "BUCKET": "data.ldn.auspatious.com",
        "SOURCE_COOP_PUBLIC_URL": "",
        "SOURCE_COOP_PREFIX_GEOMAD": "",
        "SOURCE_COOP_PREFIX_LULC": "",
        "AWS_ACCESS_KEY_ID": os.environ.get("AUSPATIOUS_AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AUSPATIOUS_AWS_SECRET_ACCESS_KEY", ""),
    },
    # TODO: Get DEP staging credentials
    # {
    #     "id": "dep-staging",
    #     "BUCKET": "dep-public-staging",
    #     "SOURCE_COOP_PUBLIC_URL": "",
    #     "SOURCE_COOP_PREFIX_GEOMAD": "",
    #     "SOURCE_COOP_PREFIX_LULC": "",
    #     "AWS_ACCESS_KEY_ID": os.environ.get("DEP_AWS_ACCESS_KEY_ID", ""),
    #     "AWS_SECRET_ACCESS_KEY": os.environ.get("DEP_AWS_SECRET_ACCESS_KEY", ""),
    # },
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

    # Source.Coop uses write credentials, other buckets use standard AWS credentials
    if config["id"] == "source-coop":
        if not os.environ.get("AWS_WRITE_ACCESS_KEY_ID"):
            pytest.skip("Source.Coop write credentials (AWS_WRITE_ACCESS_KEY_ID) not set")
    elif not config.get("AWS_ACCESS_KEY_ID"):
        pytest.skip(f"AWS credentials for {config['id']} not set")

    for k, v in config.items():
        if k != "id":
            monkeypatch.setenv(k, v)
    yield config


# TODO: Add an AM-Crossing integration test. 066_022.
TILE_ID = "010_020"  # This tile doesn't intersect with the SIDS/SPC Countries intentionally.
YEAR = "2025"
VERSION = "integration-test"


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def stac_key(bucket_env):
    source_coop_url, prefix_geomad, _ = get_source_coop_config()
    bucket = bucket_env["BUCKET"]
    tile_id_tuple = parse_tile_id(TILE_ID)

    itempath = PrefixedS3ItemPath(
        key_prefix=prefix_geomad if source_coop_url else None,
        prefix="dep",
        bucket=bucket,
        sensor=SENSOR,
        dataset_id=GEOMAD_DATASET_ID,
        version=VERSION,
        time=YEAR,
        full_path_prefix=source_coop_url if source_coop_url else f"s3://{bucket}",
    )
    return itempath.stac_path(tile_id_tuple, absolute=False)


def test_geomad_run_and_skip(bucket_env, runner, stac_key):
    """Write with overwrite, check item was recently written, then check skip doesn't overwrite."""
    source_coop_url, _, _ = get_source_coop_config()
    bucket = bucket_env["BUCKET"]
    print(bucket)

    # Use write client with Source.Coop credentials if applicable
    if source_coop_url:
        s3 = get_write_session().client("s3")
    else:
        s3 = boto3.client("s3")
    print(s3)

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
            "--integration-test",
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
            "--integration-test",
        ],
    )
    assert result.exit_code == 0, result.output

    after = s3.head_object(Bucket=bucket, Key=stac_key)["LastModified"]
    assert last_modified == after, (
        f"Item was rewritten when it should have been skipped. Before: {last_modified}, After: {after}"
    )


# TODO: Add a delete step to clean up. Don't want to leave artifacts in Source.Coop or DEP prod.
