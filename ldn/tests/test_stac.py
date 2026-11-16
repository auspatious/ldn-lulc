import tomllib
from datetime import datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from odc.geo.geobox import GeoBox
from pystac import Item
from shapely.geometry import mapping

from ldn.grids import get_gridspec
from ldn.stac import (
    PROCESSING_EXTENSION,
    PrefixedS3ItemPath,
    _add_processing_extension,
    _antimeridian_safe_bbox,
    _antimeridian_safe_geometry,
    _installed_version,
    build_pipeline_components,
    stac_providers,
)

# PrefixedS3ItemPath


def _make_pather(key_prefix=None, full_path_prefix=None) -> PrefixedS3ItemPath:
    """Construct a PrefixedS3ItemPath with the parent class stubbed out."""
    with (
        patch("dep_tools.namers.S3ItemPath.__init__", return_value=None),
    ):
        p = PrefixedS3ItemPath(key_prefix=key_prefix, full_path_prefix=full_path_prefix)
        p.key_prefix = key_prefix.strip("/") if key_prefix else None
        p.full_path_prefix = full_path_prefix
        return p


@pytest.fixture()
def pather():
    return _make_pather(key_prefix="my-prefix")


@pytest.fixture()
def pather_absolute():
    return _make_pather(key_prefix="my-prefix", full_path_prefix="s3://test-bucket")


class TestPrefixedS3ItemPath:
    def test_prefix_appears_at_start_of_relative_path(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.tif"):
            assert pather.path("item-abc").startswith("my-prefix/")

    def test_item_id_is_in_relative_path(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.tif"):
            assert "item-abc" in pather.path("item-abc")

    def test_default_extension_is_tif(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.tif"):
            assert pather.path("item-abc").endswith(".tif")

    def test_custom_extension_is_honoured(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.nc"):
            assert pather.path("item-abc", ext=".nc").endswith(".nc")

    def test_asset_name_appears_in_path_when_given(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc_red.tif"):
            assert "red" in pather.path("item-abc", asset_name="red")

    def test_no_double_slashes_in_relative_path(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.tif"):
            assert "//" not in pather.path("item-abc")

    def test_relative_path_does_not_start_with_slash(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="some/base/item-abc.tif"):
            assert not pather.path("item-abc").startswith("/")

    @pytest.mark.parametrize("raw_prefix", ["/prefix/", "prefix/"])
    def test_leading_trailing_slashes_stripped_from_prefix(self, raw_prefix):
        p = _make_pather(key_prefix=raw_prefix)
        with patch("dep_tools.namers.S3ItemPath.path", return_value="base/item-x.tif"):
            result = p.path("item-x")
        assert not result.startswith("/")
        assert "//" not in result

    def test_no_prefix_returns_parent_path_unchanged(self):
        p = _make_pather(key_prefix=None)
        with patch("dep_tools.namers.S3ItemPath.path", return_value="base/item-x.tif") as mock_super:
            result = p.path("item-x")
        assert result == mock_super.return_value

    def test_absolute_path_starts_with_full_path_prefix(self, pather_absolute):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="my-prefix/base/item-abc.tif"):
            result = pather_absolute.path("item-abc", absolute=True)
        assert result.startswith("s3://test-bucket")

    def test_absolute_path_contains_key_prefix(self, pather_absolute):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="my-prefix/base/item-abc.tif"):
            assert "my-prefix" in pather_absolute.path("item-abc", absolute=True)

    def test_absolute_path_contains_item_id(self, pather_absolute):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="my-prefix/base/item-abc.tif"):
            assert "item-abc" in pather_absolute.path("item-abc", absolute=True)

    def test_absolute_true_without_full_path_prefix_gives_relative(self, pather):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="my-prefix/base/item-abc.tif"):
            result = pather.path("item-abc", absolute=True)
        assert not result.startswith("s3://")

    def test_absolute_path_has_no_double_slashes(self, pather_absolute):
        with patch("dep_tools.namers.S3ItemPath.path", return_value="my-prefix/base/item-abc.tif"):
            result = pather_absolute.path("item-abc", absolute=True)
        assert "//" not in result.replace("s3://", "")


# build_pipeline_components
_MOD = "ldn.stac"

TILE = (66, 22)
YEAR = "2025"
VERSION = "test"
OWNER = "dep"


@pytest.fixture(autouse=False)
def mock_aws():
    """Mock all AWS I/O so no credentials are needed."""
    with patch(f"{_MOD}.object_exists", return_value=False) as mock_object_exists:
        yield mock_object_exists


def test_build_pipeline_components_uses_public_url_base_for_full_path_prefix(mock_aws):
    """build_pipeline_components should call get_public_url_base(bucket) and pass its
    return value through to itempath.full_path_prefix — proves the wiring, not the URL logic."""
    with patch(f"{_MOD}.get_public_url_base", return_value="https://example-base.com") as mock_get_url:
        result = build_pipeline_components(
            TILE,
            YEAR,
            VERSION,
            "dep-public-staging",
            OWNER,
            "geomad",
            None,
            overwrite=True,
            collection_url_root="https://example.com/collections",
            s3_client=MagicMock(),
            sensor="ls",
        )
    mock_get_url.assert_called_once_with("dep-public-staging")
    itempath, *_ = result
    assert itempath.full_path_prefix == "https://example-base.com"


@pytest.mark.parametrize(
    "bucket,source_coop_prefix,expected_key_prefix",
    [
        ("us-west-2.opendata.source.coop", "auspatious/geomad-sids", "auspatious/geomad-sids"),
        ("data.ldn.auspatious.com", None, None),
        ("dep-public-staging", None, None),
    ],
)
def test_build_pipeline_components_itempath_key_prefix(mock_aws, bucket, source_coop_prefix, expected_key_prefix):
    """key_prefix on itempath should only be set for Source.Coop."""
    result = build_pipeline_components(
        TILE,
        YEAR,
        VERSION,
        bucket,
        OWNER,
        "geomad",
        source_coop_prefix,
        overwrite=True,
        collection_url_root="https://example.com/collections",
        s3_client=MagicMock(),
        sensor="ls",
    )
    assert result is not None
    itempath, *_ = result
    assert itempath.key_prefix == expected_key_prefix


def test_build_pipeline_components_returns_none_when_exists(mock_aws):
    """Should return None and skip processing when item exists and overwrite=False."""
    with patch(f"{_MOD}.object_exists", return_value=True):
        result = build_pipeline_components(
            TILE,
            YEAR,
            VERSION,
            "dep-public-staging",
            OWNER,
            "geomad",
            None,
            overwrite=False,
            collection_url_root="https://example.com/collections",
            s3_client=MagicMock(),
            sensor="ls",
        )
    assert result is None


def test_build_pipeline_components_proceeds_when_exists_and_overwrite(mock_aws):
    """overwrite=True should proceed even when item exists."""
    with patch(f"{_MOD}.object_exists", return_value=True):
        result = build_pipeline_components(
            TILE,
            YEAR,
            VERSION,
            "dep-public-staging",
            OWNER,
            "geomad",
            None,
            overwrite=True,
            collection_url_root="https://example.com/collections",
            s3_client=MagicMock(),
            sensor="ls",
        )
    assert result is not None


def test_add_processing_extension():
    item = Item(id="t", geometry=None, bbox=None, datetime=datetime(2020, 1, 1), properties={})
    providers = stac_providers("lulc", source_coop=False)
    _add_processing_extension(item, ["ldn", "pystac"], providers)
    _add_processing_extension(item, ["ldn", "pystac"], providers)  # Idempotent extension list.
    assert item.stac_extensions.count(PROCESSING_EXTENSION) == 1
    assert set(item.properties["processing:software"]) == {"ldn", "pystac"}
    assert item.properties["processing:version"] == _installed_version("ldn")
    # RFC 3339 in UTC, as the extension requires.
    processed = datetime.fromisoformat(item.properties["processing:datetime"])
    assert item.properties["processing:datetime"].endswith("Z") and processed.utcoffset() == timedelta(0)
    assert [p.name for p in item.common_metadata.providers] == ["Digital Earth Pacific", "Auspatious", "USGS", "NASA"]
    auspatious = item.common_metadata.providers[1]
    assert auspatious.extra_fields["processing:facility"] == "Digital Earth Pacific"
    assert auspatious.extra_fields["processing:level"] == "L4"


def test_installed_version_appends_pinned_git_commit():
    """Git dependencies report the commit pinned in pyproject.toml, so STAC processing:software records it."""
    assert _installed_version("numpy") == np.__version__
    sources = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())["tool"]["uv"]["sources"]
    for name in ["dep-tools", "datacube-compute"]:
        assert _installed_version(name) == f"{version(name)}+{sources[name]['rev']}"


# Antimeridian-safe bbox and geometry

# EPSG:3832 (WGS 84 / PDC Mercator) x=3_330_000 is roughly where longitude
# wraps from +180 to -180 at this latitude.
_AM_CROSSING_BBOX_3832 = (3_285_000, -2_015_000, 3_375_000, -1_925_000)
_NON_CROSSING_BBOX_3832 = (0, -2_015_000, 90_000, -1_925_000)


def test_antimeridian_safe_bbox_flips_crossing_tile() -> None:
    """A tile straddling the antimeridian gets a flipped bbox (bbox[0] > bbox[2])."""
    gb = GeoBox.from_bbox(_AM_CROSSING_BBOX_3832, crs="EPSG:3832", resolution=100)

    fixed = _antimeridian_safe_bbox(gb, fallback_bbox=[-180, -18, 180, -17])

    assert fixed[0] > fixed[2], "expected STAC antimeridian convention: east edge > west edge"
    assert fixed[0] == 179.50965708332626
    assert fixed[2] == -179.68185916096616


def test_antimeridian_safe_bbox_passes_through_non_crossing_tile() -> None:
    """A tile that doesn't cross the antimeridian keeps rio_stac's original bbox."""
    gb = GeoBox.from_bbox(_NON_CROSSING_BBOX_3832, crs="EPSG:3832", resolution=100)
    fallback = [149.9, -18, 150.1, -17]

    assert _antimeridian_safe_bbox(gb, fallback_bbox=fallback) == fallback


def test_antimeridian_safe_geometry_splits_crossing_tile() -> None:
    """Tile 066_020 crosses the antimeridian, so its footprint is split into a MultiPolygon."""
    tile = get_gridspec("pacific").tile_geobox((66, 20))
    footprint = mapping(tile.extent.to_crs("EPSG:4326").geom)
    assert _antimeridian_safe_geometry(tile, footprint)["type"] == "MultiPolygon"


def test_antimeridian_safe_geometry_keeps_small_area_next_to_antimeridian() -> None:
    """The 5 x 5 pixel corner of tile 066_020 is just west of the antimeridian and doesn't cross it.

    This counter-clockwise ring (as rio_stac writes it) made antimeridian.fix_shape raise.
    """
    corner = get_gridspec("pacific").tile_geobox((66, 20))[0:5, 0:5]
    west, south, east, north = 179.967798, -17.654105, 179.969145, -17.652813
    footprint = {
        "type": "Polygon",
        "coordinates": [[(west, south), (east, south), (east, north), (west, north), (west, south)]],
    }
    assert _antimeridian_safe_geometry(corner, footprint) == footprint
