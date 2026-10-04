from odc.geo.geobox import GeoBox

from ldn.raster import _antimeridian_safe_bbox

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
