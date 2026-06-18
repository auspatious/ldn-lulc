# Seperate folder for integration tests so they can be easily excluded from regular test runs.
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from ldn.cli_geomad import geomad_app
from ldn.raster import PrefixedS3ItemPath
from ldn.utils import GEOMAD_DATASET_ID, SENSOR, SOURCE_COOP_PREFIX_GEOMAD, is_source_coop, parse_tile_id

pytestmark = pytest.mark.integration

INTEGRATION_CONFIGS = [
    {
        "id": "auspatious",
        "BUCKET": "data.ldn.auspatious.com",
        "SOURCE_COOP_URL": "",
    },
    {
        "id": "private-bucket",
        "BUCKET": "dep-public-staging",
        "SOURCE_COOP_URL": "",
    },
    # This won't work locally because the role is only available in Argo/Kubernetes.
    # {
    #     "id": "source-coop",
    #     "BUCKET": "us-west-2.opendata.source.coop",
    #     "SOURCE_COOP_URL": "https://data.source.coop",
    # },
]


@pytest.fixture(params=INTEGRATION_CONFIGS, ids=[c["id"] for c in INTEGRATION_CONFIGS])
def bucket_env(request, monkeypatch):
    config = request.param

    # moto still expects credentials to be present in the environment.
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
        stac_key = self.stac_writer.itempath.stac_path(self.id, absolute=False)
        self.stac_writer.client.put_object(
            Bucket=self.stac_writer.itempath.bucket,
            Key=stac_key,
            Body=b"{}",
            ContentType="application/json",
        )
        return [stac_key]

    monkeypatch.setattr("ldn.cli_geomad.Task.run", fake_run)


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
    _is_source_coop = is_source_coop()
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
    """Write with overwrite, check item was recently written, then check skip doesn't overwrite."""
    bucket = bucket_env["BUCKET"]
    print(bucket)

    s3 = mock_s3
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
