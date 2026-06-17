# Seperate folder for integration tests so they can be easily excluded from regular test runs.
import logging
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

logger = logging.getLogger(__name__)

INTEGRATION_CONFIGS = [
    {
        "id": "auspatious",
        "BUCKET": "data.ldn.auspatious.com",
        "SOURCE_COOP_PUBLIC_URL": "",
        "SOURCE_COOP_PREFIX_GEOMAD": "",
        "SOURCE_COOP_PREFIX_LULC": "",
        "AWS_ACCESS_KEY_ID": os.environ.get("AUSPATIOUS_AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AUSPATIOUS_AWS_SECRET_ACCESS_KEY", ""),
        "AWS_SESSION_TOKEN": os.environ.get("AUSPATIOUS_AWS_SESSION_TOKEN", ""),
    },
    {
        "id": "dep-staging",
        "BUCKET": "dep-public-staging",
        "SOURCE_COOP_PUBLIC_URL": "",
        "SOURCE_COOP_PREFIX_GEOMAD": "",
        "SOURCE_COOP_PREFIX_LULC": "",
        "AWS_ACCESS_KEY_ID": os.environ.get("DEP_AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("DEP_AWS_SECRET_ACCESS_KEY", ""),
        "AWS_SESSION_TOKEN": os.environ.get("DEP_AWS_SESSION_TOKEN", ""),
    },
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
        if not os.environ.get("SOURCE_COOP_AWS_ACCESS_KEY_ID"):
            pytest.skip("Source.Coop write credentials (SOURCE_COOP_AWS_ACCESS_KEY_ID) not set")
    elif not config.get("AWS_ACCESS_KEY_ID"):
        pytest.skip(f"AWS credentials for {config['id']} not set")

    for k, v in config.items():
        if k != "id":
            monkeypatch.setenv(k, v)
    yield config


# TODO: Add an AM-Crossing integration test. 066_022.
# TODO: Test a non-pacific tile as well. One that doesn't intersect countries.
TILE_ID = "010_020"  # This tile (in Australia) doesn't intersect with the SIDS/SPC Countries intentionally.
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


# Delete/clean-up step. Don't want to leave files in Source.Coop or DEP prod.
# This runs after every test automatically via autouse=True
@pytest.fixture(autouse=True)
def cleanup_stac_item(bucket_env, stac_key):
    """Delete the test STAC item and assets after each test."""
    yield
    source_coop_url, _, _ = get_source_coop_config()
    bucket = bucket_env["BUCKET"]

    # Use the same credentials that were used to write
    if source_coop_url:
        s3 = get_write_session().client("s3")
    else:
        s3 = boto3.Session(
            aws_access_key_id=bucket_env.get("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=bucket_env.get("AWS_SECRET_ACCESS_KEY") or None,
            aws_session_token=bucket_env.get("AWS_SESSION_TOKEN") or None,
            region_name="us-west-2",
        ).client("s3")

    folder = stac_key.rsplit("/", 1)[0] + "/"
    logger.info(f"Cleaning up test folder: {folder}")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
    contents = response.get("Contents", [])
    len_contents = len(contents)
    count_expected = 11
    # Safeguard to not delete too many files
    assert len_contents == count_expected, (
        f"There should be {count_expected} files to clean up (1 STAC item + 10 assets), found {len_contents}."
    )

    count_deleted = 0
    for obj in contents:
        key = obj["Key"]
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            count_deleted += 1
            logger.info(f"Deleted test item: {key}")
        except Exception as e:
            logger.warning(f"Failed to delete test item {key}: {e}")

    assert count_deleted == count_expected, f"Expected to delete {count_expected} files, but deleted {count_deleted}."
