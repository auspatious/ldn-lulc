from ldn.utils import (
    AWS_REGION,
    BUCKET,
    GEOMAD_VERSION,
    NON_PACIFIC_OWNER,
    PACIFIC_OWNER,
    SOURCE_COOP_PREFIX_GEOMAD,
    SOURCE_COOP_PUBLIC_URL,
    dataset_prefix,
    get_geomad_stac_geoparquet_url,
    owner_for_region,
)


class TestOwnerForRegion:
    def test_pacific_default(self):
        assert owner_for_region("pacific") == PACIFIC_OWNER

    def test_non_pacific_default(self):
        assert owner_for_region("non-pacific") == NON_PACIFIC_OWNER

    def test_pacific_custom(self):
        assert owner_for_region("pacific", "x", "y") == "x"

    def test_non_pacific_custom(self):
        assert owner_for_region("non-pacific", "x", "y") == "y"

    def test_product_owner_overrides_pacific(self):
        assert owner_for_region("pacific", "dep", "ci", product_owner="custom") == "custom"

    def test_product_owner_overrides_non_pacific(self):
        assert owner_for_region("non-pacific", "dep", "ci", product_owner="custom") == "custom"

    def test_product_owner_none_uses_region(self):
        assert owner_for_region("pacific", "dep", "ci", product_owner=None) == "dep"


class TestDatasetPrefix:
    def test_geomad(self):
        assert dataset_prefix("dep", "geomad") == "dep_ls_geomad"

    def test_prediction(self):
        assert dataset_prefix("ci", "lulc_prediction") == "ci_ls_lulc_prediction"


class TestGetGeomadStacGeoparquetUrl:
    def test_pacific(self):
        url = get_geomad_stac_geoparquet_url("pacific", bucket=BUCKET)
        expected = (
            f"https://s3.{AWS_REGION}.amazonaws.com/{BUCKET}/dep_ls_geomad/{GEOMAD_VERSION}/dep_ls_geomad.parquet"
        )
        if SOURCE_COOP_PUBLIC_URL:
            expected = (
                f"{SOURCE_COOP_PUBLIC_URL}/{SOURCE_COOP_PREFIX_GEOMAD}/"
                f"dep_ls_geomad/{GEOMAD_VERSION}/dep_ls_geomad.parquet"
            )
        assert url == expected

    def test_non_pacific(self):
        url = get_geomad_stac_geoparquet_url("non-pacific", bucket=BUCKET)
        expected = f"https://s3.{AWS_REGION}.amazonaws.com/{BUCKET}/ci_ls_geomad/{GEOMAD_VERSION}/ci_ls_geomad.parquet"
        if SOURCE_COOP_PUBLIC_URL:
            expected = (
                f"{SOURCE_COOP_PUBLIC_URL}/{SOURCE_COOP_PREFIX_GEOMAD}/"
                f"ci_ls_geomad/{GEOMAD_VERSION}/ci_ls_geomad.parquet"
            )
        assert url == expected

    def test_product_owner_override(self):
        url = get_geomad_stac_geoparquet_url("pacific", product_owner="ci", bucket=BUCKET)
        expected = f"https://s3.{AWS_REGION}.amazonaws.com/{BUCKET}/ci_ls_geomad/{GEOMAD_VERSION}/ci_ls_geomad.parquet"
        if SOURCE_COOP_PUBLIC_URL:
            expected = (
                f"{SOURCE_COOP_PUBLIC_URL}/{SOURCE_COOP_PREFIX_GEOMAD}/ci_ls_geomad"
                f"/{GEOMAD_VERSION}/ci_ls_geomad.parquet"
            )
        assert url == expected
