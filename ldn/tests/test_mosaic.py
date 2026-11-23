from datetime import UTC, datetime

import pytest
from pystac import Item

from ldn.grids import get_gridspec
from ldn.mosaic import country_polygon, country_region, mosaic_key_stem, mosaic_stac_item
from ldn.utils import LdnError


def test_fiji_covers_tiles_across_the_antimeridian():
    """Fiji's polygon is continuous in the Pacific grid, so it covers the tiles either side of the antimeridian."""
    region = country_region("FJI")
    tiles = {tile for tile, _ in get_gridspec(region).tiles_from_geopolygon(country_polygon("FJI", region))}

    assert region == "pacific"
    assert {(65, 20), (66, 20), (67, 20)} <= tiles


def test_non_pacific_country_region():
    """A country outside the DEP grid is on the non-Pacific grid."""
    assert country_region("BRB") == "non-pacific"


def test_unknown_country_raises():
    """Codes outside the project raise."""
    with pytest.raises(LdnError):
        country_region("AUS")


def test_mosaic_key_stem():
    """Mosaics are next to the index, under the region's owner only for single region products."""
    assert (
        mosaic_key_stem("FJI", "2020", "geomad", "0-3-0", "dep-public-staging", True, None, "ls")
        == "dep_ls_geomad/0-3-0/mosaic/2020/dep_ls_geomad_FJI_2020"
    )
    assert (
        mosaic_key_stem("BRB", "2020", "lulc", "0-2-1", "us-west-2.opendata.source.coop", False, None, "ls")
        == "auspatious/lulc-sids/ls_lulc/0-2-1/mosaic/2020/ls_lulc_BRB_2020"
    )


def test_fiji_stac_item_bbox_crosses_the_antimeridian():
    """The Fiji mosaic's bbox is in the antimeridian-crossing form (west edge greater than east edge)."""
    region = country_region("FJI")
    geobox = get_gridspec(region).tile_geobox((66, 20))
    input_item = Item("in", None, None, datetime(2020, 6, 30, tzinfo=UTC), {})
    stem = "dep_ls_geomad/0-3-0/mosaic/2020/dep_ls_geomad_FJI_2020"

    item = mosaic_stac_item(stem, geobox, ["red", "rgb"], [input_item], "geomad", "dep-public-staging")

    assert item.id == "dep_ls_geomad_FJI_2020"
    assert item.bbox[0] > item.bbox[2]
    assert item.assets["rgb"].roles == ["visual"]
    assert item.get_self_href().endswith(f"{stem}.stac-item.json")
