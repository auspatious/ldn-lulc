from datetime import UTC, datetime, timedelta

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from ldn.cli_geomad import geomad_app
from ldn.raster import PrefixedS3ItemPath
from ldn.utils import GEOMAD_DATASET_ID, SENSOR, SOURCE_COOP_PREFIX_GEOMAD, get_bool_env_var, parse_tile_id

SMOKE_CONFIGS = [
    {
        "id": "auspatious",
        "BUCKET": "data.ldn.auspatious.com",
        "IS_SOURCE_COOP": "false",
    },
    {
        "id": "private-bucket",
        "BUCKET": "dep-public-staging",
        "IS_SOURCE_COOP": "false",
    },
]


@pytest.fixture(params=SMOKE_CONFIGS, ids=[c["id"] for c in SMOKE_CONFIGS])
def bucket_env(request, monkeypatch):
    """Set per-bucket environment variables for the CLI smoke test."""
    config = request.param

    # moto requires credential env vars to be present.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")

    for k, v in config.items():
        if k != "id":
            monkeypatch.setenv(k, v)
    yield config


@pytest.fixture
def mock_s3(bucket_env):
    """Create a moto-backed S3 client and target bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(
            Bucket=bucket_env["BUCKET"],
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        yield s3


@pytest.fixture(autouse=True)
def stub_geomad_processing(monkeypatch):
    """Avoid external data calls while exercising CLI write/skip behavior."""

    monkeypatch.setattr("ldn.cli_geomad._count_scenes", lambda **_: 25)

    def fake_run(self):
        stac_key = self.stac_writer._itempath.stac_path(self.id, absolute=False)
        boto3.client("s3", region_name="us-west-2").put_object(
            Bucket=self.stac_writer._itempath.bucket,
            Key=stac_key,
            Body=b"{}",
            ContentType="application/json",
        )
        return [stac_key]

    monkeypatch.setattr("ldn.cli_geomad.Task.run", fake_run)


TILE_ID = "010_020"
YEAR = "2025"
VERSION = "integration-test"


@pytest.fixture
def runner():
    """Return CLI runner."""
    return CliRunner()


@pytest.fixture
def stac_key(bucket_env):
    """Build expected STAC key for the test tile/year."""
    _is_source_coop = get_bool_env_var("IS_SOURCE_COOP")
    bucket = bucket_env["BUCKET"]
    tile_id_tuple = parse_tile_id(TILE_ID)

    itempath = PrefixedS3ItemPath(
        key_prefix=SOURCE_COOP_PREFIX_GEOMAD if _is_source_coop else None,
        prefix="dep",
        bucket=bucket,
        sensor=SENSOR,
        dataset_id=GEOMAD_DATASET_ID,
        version=VERSION,
        time=YEAR,
        full_path_prefix=SOURCE_COOP_PREFIX_GEOMAD if _is_source_coop else f"s3://{bucket}",
    )
    return itempath.stac_path(tile_id_tuple, absolute=False)


def test_geomad_run_and_skip(bucket_env, mock_s3, runner, stac_key):
    """Write with overwrite, then verify second run skips rewrite."""
    bucket = bucket_env["BUCKET"]
    s3 = mock_s3

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

    response = s3.head_object(Bucket=bucket, Key=stac_key)
    last_modified = response["LastModified"]
    assert datetime.now(UTC) - last_modified < timedelta(minutes=15), (
        f"STAC item was not recently written: {last_modified}"
    )

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
