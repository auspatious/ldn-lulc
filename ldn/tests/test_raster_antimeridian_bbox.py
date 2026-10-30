from odc.geo.geobox import GeoBox
from shapely.geometry import mapping

from ldn.grids import get_gridspec
from ldn.raster import _antimeridian_safe_bbox, _antimeridian_safe_geometry

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
