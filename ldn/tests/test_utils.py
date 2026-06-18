from unittest.mock import patch

import pytest

from ldn.utils import (
    AWS_REGION,
    NON_PACIFIC_OWNER,
    PACIFIC_OWNER,
    SOURCE_COOP_PREFIX_GEOMAD,
    SOURCE_COOP_PREFIX_LULC,
    LdnError,
    dataset_prefix,
    get_collection_url_root,
    get_full_path_prefix,
    get_geomad_stac_geoparquet_url,
    get_public_https_prefix,
    owner_for_region,
    parse_tile_id,
    parse_years,
    resolve_dataset,
)


class TestOwnerForRegion:
    def test_pacific_default(self):
        assert owner_for_region("pacific") == PACIFIC_OWNER

    def test_non_pacific_default(self):
        assert owner_for_region("non-pacific") == NON_PACIFIC_OWNER

    def test_product_owner_overrides_pacific(self):
        assert owner_for_region("pacific", product_owner="custom") == "custom"

    def test_product_owner_overrides_non_pacific(self):
        assert owner_for_region("non-pacific", product_owner="custom") == "custom"

    def test_product_owner_none_uses_region(self):
        assert owner_for_region("pacific", product_owner=None) == PACIFIC_OWNER


class TestDatasetPrefix:
    def test_geomad(self):
        assert dataset_prefix("dep", "geomad") == "dep_ls_geomad"

    def test_lulc(self):
        assert dataset_prefix("ci", "lulc") == "ci_ls_lulc"


MODULE = "ldn.utils"

MOCK_BUCKET = "my-test-bucket"
MOCK_REGION = "ap-southeast-2"
MOCK_VERSION = "0-0-1"
MOCK_DATASET_ID = "geomad"
MOCK_SOURCE_COOP_URL = "https://data.source.coop"
# TODO: Do these need mocking?
MOCK_SOURCE_COOP_PREFIX = "auspatious/geomad-sids"
MOCK_SOURCE_COOP_PREFIX_LULC = "auspatious/lulc-sids"

NO_SOURCE_COOP = (None, None, None)
WITH_SOURCE_COOP = (MOCK_SOURCE_COOP_URL,)


@pytest.fixture
def base_patches():
    with (
        patch(f"{MODULE}.AWS_REGION", MOCK_REGION),
        patch(f"{MODULE}.GEOMAD_DATASET_ID", MOCK_DATASET_ID),
    ):
        yield


class TestGetGeomadStacGeoparquetUrl:
    @pytest.mark.parametrize(
        "bucket,source_coop_url,expected",
        [
            (
                "us-west-2.opendata.source.coop",
                MOCK_SOURCE_COOP_URL,
                f"{MOCK_SOURCE_COOP_URL}/{MOCK_SOURCE_COOP_PREFIX}/ls_geomad/{MOCK_VERSION}/ls_geomad.parquet",
            ),
            (
                "data.ldn.auspatious.com",
                "",
                f"https://s3.{MOCK_REGION}.amazonaws.com/data.ldn.auspatious.com/{MOCK_SOURCE_COOP_PREFIX}/ls_geomad/{MOCK_VERSION}/ls_geomad.parquet",
            ),
            (
                "dep-public-staging",
                "",
                f"https://s3.{MOCK_REGION}.amazonaws.com/dep-public-staging/{MOCK_SOURCE_COOP_PREFIX}/ls_geomad/{MOCK_VERSION}/ls_geomad.parquet",
            ),
        ],
    )
    def test_bucket_styles(self, base_patches, bucket, source_coop_url, expected):
        with patch(f"{MODULE}.get_env_var", return_value=source_coop_url):
            url = get_geomad_stac_geoparquet_url(bucket=bucket, version=MOCK_VERSION)
        assert url == expected


# parse_years tests


@pytest.mark.parametrize(
    "tile_id,expected",
    [
        ("028_030", (28, 30)),
        ("28_30", (28, 30)),
        ("28,30", (28, 30)),
        ("28-30", (28, 30)),
        ("0_0", (0, 0)),
        ("066_022", (66, 22)),
    ],
)
def test_parse_tile_id_valid(tile_id, expected):
    assert parse_tile_id(tile_id) == expected


@pytest.mark.parametrize(
    "tile_id",
    [
        "28",
        "28_30_40",
        "",
        "abc_def",
    ],
)
def test_parse_tile_id_invalid(tile_id):
    with pytest.raises((LdnError, ValueError)):
        parse_tile_id(tile_id)


@pytest.mark.parametrize(
    "years,expected",
    [
        # Single year
        ("2020", [2020]),
        # Comma-separated
        ("2020,2021", [2020, 2021]),
        ("2020,2021,2022", [2020, 2021, 2022]),
        ("2020, 2021", [2020, 2021]),  # with spaces
        # Range
        ("2010-2023", list(range(2010, 2024))),
        ("2020-2020", [2020]),  # single-year range
        ("2021-2022", [2021, 2022]),
    ],
)
def test_parse_years_valid(years, expected):
    assert parse_years(years) == expected


@pytest.mark.parametrize(
    "years",
    [
        "abc",
        "2020,abc",
        "2020-abc",
        "",
    ],
)
def test_parse_years_invalid(years):
    with pytest.raises(ValueError):
        parse_years(years)


MODULE = "ldn.utils"


@pytest.fixture(autouse=True)
def mock_constants():
    with (
        patch(f"{MODULE}.SOURCE_COOP_PREFIX_GEOMAD", "auspatious/geomad-sids"),
        patch(f"{MODULE}.SOURCE_COOP_PREFIX_LULC", "auspatious/lulc-sids"),
    ):
        yield


def test_resolve_dataset_geomad():
    dataset_id, version, prefix = resolve_dataset("geomad", "0.0.1", "0.0.2")
    assert dataset_id == "geomad"
    assert version == "0.0.1"
    assert prefix == "auspatious/geomad-sids"


def test_resolve_dataset_lulc():
    dataset_id, version, prefix = resolve_dataset("lulc", "0.0.1", "0.0.2")
    assert dataset_id == "lulc"
    assert version == "0.0.2"
    assert prefix == "auspatious/lulc-sids"


def test_resolve_dataset_geomad_ignores_lulc_version():
    _, version, _ = resolve_dataset("geomad", "1.0.0", "9.9.9")
    assert version == "1.0.0"


def test_resolve_dataset_lulc_ignores_geomad_version():
    _, version, _ = resolve_dataset("lulc", "9.9.9", "1.0.0")
    assert version == "1.0.0"


def test_resolve_dataset_prefix_from_constants():
    _, _, geomad_prefix = resolve_dataset("geomad", "0.0.1", "0.0.2")
    _, _, lulc_prefix = resolve_dataset("lulc", "0.0.1", "0.0.2")
    assert geomad_prefix == SOURCE_COOP_PREFIX_GEOMAD
    assert lulc_prefix == SOURCE_COOP_PREFIX_LULC


@pytest.mark.parametrize(
    "bucket,is_source_coop_bucket,expected",
    [
        (
            "us-west-2.opendata.source.coop",
            True,
            "https://data.source.coop",
        ),
        (
            "data.ldn.auspatious.com",
            False,
            "s3://data.ldn.auspatious.com",
        ),
        (
            "dep-public-staging",
            False,
            "s3://dep-public-staging",
        ),
    ],
)
def test_get_full_path_prefix(bucket, is_source_coop_bucket, expected):
    with (
        patch(f"{MODULE}.is_source_coop", return_value=is_source_coop_bucket),
        patch(f"{MODULE}.get_env_var", return_value=MOCK_SOURCE_COOP_URL),
    ):
        assert get_full_path_prefix(bucket) == expected


@pytest.mark.parametrize(
    "bucket,is_source_coop_bucket,expected",
    [
        (
            "us-west-2.opendata.source.coop",
            True,
            "https://data.source.coop/#dep_ls_geomad/",
        ),
        (
            "data.ldn.auspatious.com",
            False,
            "https://data.ldn.auspatious.com/#dep_ls_geomad/",
        ),
        (
            "dep-public-staging",
            False,
            f"https://s3.{AWS_REGION}.amazonaws.com/dep-public-staging/#dep_ls_geomad/",
        ),
    ],
)
def test_get_collection_url_root(bucket, is_source_coop_bucket, expected):
    with (
        patch(f"{MODULE}.is_source_coop", return_value=is_source_coop_bucket),
        patch(f"{MODULE}.get_env_var", return_value=MOCK_SOURCE_COOP_URL),
    ):
        assert get_collection_url_root(bucket, "dep", "ls", "geomad") == expected


@pytest.mark.parametrize(
    "bucket,is_source_coop_bucket,expected",
    [
        (
            "us-west-2.opendata.source.coop",
            True,
            "https://data.source.coop",
        ),
        (
            "data.ldn.auspatious.com",
            False,
            "https://data.ldn.auspatious.com",
        ),
        (
            "dep-public-staging",
            False,
            f"https://s3.{AWS_REGION}.amazonaws.com/dep-public-staging",
        ),
    ],
)
def test_get_public_https_prefix(bucket, is_source_coop_bucket, expected):
    with (
        patch(f"{MODULE}.is_source_coop", return_value=is_source_coop_bucket),
        patch(f"{MODULE}.get_env_var", return_value=MOCK_SOURCE_COOP_URL),
    ):
        assert get_public_https_prefix(bucket) == expected


def test_parse_years_reversed_range():
    with pytest.raises(ValueError, match="Start year must be <= end year"):
        parse_years("2023-2020")


def test_parse_years_same_year_range():
    assert parse_years("2020-2020") == [2020]


@pytest.mark.parametrize(
    "owner,expected",
    [
        ("dep", "dep_ls_geomad"),
        ("ci", "ci_ls_geomad"),
        (None, "ls_geomad"),
    ],
)
def test_dataset_prefix(owner, expected):
    with patch(f"{MODULE}.SENSOR", "ls"):
        assert dataset_prefix(owner, "geomad") == expected
