import tomllib
from datetime import datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr
from dep_tools.utils import bbox_across_180
from geopandas import GeoDataFrame
from odc.geo.geobox import GeoBox
from odc.geo.xr import assign_crs, xr_zeros
from planetary_computer import sign_url
from pyproj import Geod, Transformer
from pystac import Item
from pystac.extensions.projection import ProjectionExtension

from ldn.raster import (
    PROCESSING_EXTENSION,
    PrefixedS3ItemPath,
    _calculate_terrain_features,
    _merge_geographic_halves,
    add_processing_extension,
    build_pipeline_components,
    calculate_indices,
    installed_version,
    load_across_antimeridian,
    load_dem_terrain,
    safe_divide,
    scale_offset_landsat,
    stac_providers,
)
from ldn.utils import LdnError

# Test-data factories  (no mocks, no I/O)


def _band_dataset(**bands: np.ndarray) -> xr.Dataset:
    """Build a Dataset from keyword-name to 2-D uint16 array, with nodata 0 declared like the GeoMAD STAC items."""
    return xr.Dataset({name: (("y", "x"), arr.astype(np.uint16), {"nodata": 0}) for name, arr in bands.items()})


def _spectral_dataset(nir=0.8, red=0.2, green=0.4, blue=0.1, swir16=0.5, swir22=0.3) -> xr.Dataset:
    """1x1 float Dataset with all bands required by calculate_indices."""
    return xr.Dataset(
        {
            "nir08": xr.DataArray([[nir]]),
            "red": xr.DataArray([[red]]),
            "green": xr.DataArray([[green]]),
            "blue": xr.DataArray([[blue]]),
            "swir16": xr.DataArray([[swir16]]),
            "swir22": xr.DataArray([[swir22]]),
        }
    )


# DEMs on a 1 arcsecond WGS84 grid, like Copernicus DEM GLO-30.
ARCSEC = 1 / 3600


def _pixel_m(lat: float, east: bool) -> float:
    """Ground distance in metres of one pixel east (or north) at a latitude, from pyproj's geodesic."""
    return Geod(ellps="WGS84").inv(170, lat, 170 + ARCSEC * east, lat + ARCSEC * (not east))[2]


def _geo_dem(values: np.ndarray, lat: float = 0.0) -> xr.DataArray:
    """North-up DEM centred on latitude lat, with x/y in degrees."""
    rows, cols = values.shape
    dem = xr.DataArray(
        values.astype(np.float32),
        dims=("y", "x"),
        coords={
            "x": 170.0 + np.arange(cols) * ARCSEC,
            "y": lat + (np.arange(rows)[::-1] - rows // 2) * ARCSEC,
        },
        name="elevation",
    )
    return assign_crs(dem, "EPSG:4326")


def _flat_dem(size=5, elevation=100.0) -> xr.DataArray:
    return _geo_dem(np.full((size, size), elevation))


def _x_ramp_dem(lat: float = 0.0) -> xr.DataArray:
    """3x3 DEM rising by exactly 1 m per pixel eastward, flat north-south."""
    return _geo_dem(np.array([[100, 101, 102]] * 3), lat)


def _y_ramp_dem(lat: float = 0.0) -> xr.DataArray:
    """3x3 DEM rising by exactly 1 m per pixel northward, flat east-west."""
    return _geo_dem(np.array([[102] * 3, [101] * 3, [100] * 3]), lat)


# Landsat scaling  - pure numpy, zero mocks

SCALE = 0.0000275
OFFSET = -0.2


class TestScaleOffsetLandsat:
    """
    Tests the formula, the declared nodata, clipping, dtype, and that
    the non-spectral bands (count/emad/smad/bcmad) are left untouched.
    """

    def test_valid_pixel_applies_usgs_formula(self):
        raw = 10_000
        ds = _band_dataset(red=np.array([[raw]]))
        result = scale_offset_landsat(ds)
        expected = float(np.clip(raw * SCALE + OFFSET, 0, 1))
        assert result["red"].item() == pytest.approx(expected)

    def test_declared_nodata_becomes_nan(self):
        ds = _band_dataset(red=np.array([[0]]))
        assert np.isnan(scale_offset_landsat(ds)["red"].item())

    def test_missing_nodata_raises(self):
        ds = xr.Dataset({"red": (("y", "x"), np.array([[0]], dtype=np.uint16))})
        with pytest.raises(LdnError, match="no declared nodata"):
            scale_offset_landsat(ds)

    def test_low_raw_value_clips_to_zero(self):
        # 1 * 0.0000275 − 0.2 = −0.19997…  → clipped to 0
        ds = _band_dataset(red=np.array([[1]]))
        assert scale_offset_landsat(ds)["red"].item() == pytest.approx(0.0)

    def test_high_raw_value_clips_to_one(self):
        # 50 000 * 0.0000275 − 0.2 = 1.175  → clipped to 1
        ds = _band_dataset(red=np.array([[50_000]]))
        assert scale_offset_landsat(ds)["red"].item() == pytest.approx(1.0)

    def test_output_dtype_is_float32(self):
        ds = _band_dataset(red=np.array([[10_000]]))
        assert scale_offset_landsat(ds)["red"].dtype == np.float32

    @pytest.mark.parametrize("passthrough_band", ["count", "emad", "smad", "bcmad"])
    def test_non_spectral_bands_pass_through_unmodified(self, passthrough_band):
        ds = _band_dataset(red=np.array([[10_000]]), **{passthrough_band: np.array([[42]])})
        result = scale_offset_landsat(ds)
        assert result[passthrough_band].item() == 42
        assert result[passthrough_band].dtype == np.uint16

    def test_multiple_spectral_bands_scaled_independently(self):
        ds = _band_dataset(red=np.array([[10_000]]), nir08=np.array([[20_000]]))
        result = scale_offset_landsat(ds)
        assert result["red"].item() == pytest.approx(float(np.clip(10_000 * SCALE + OFFSET, 0, 1)))
        assert result["nir08"].item() == pytest.approx(float(np.clip(20_000 * SCALE + OFFSET, 0, 1)))

    def test_nodata_and_valid_coexist_in_same_band(self):
        ds = _band_dataset(red=np.array([[0, 10_000]]))
        red = scale_offset_landsat(ds)["red"].values[0]
        assert np.isnan(red[0])
        assert red[1] == pytest.approx(float(np.clip(10_000 * SCALE + OFFSET, 0, 1)))

    def test_mutates_and_returns_same_dataset_object(self):
        ds = _band_dataset(red=np.array([[10_000]]))
        assert scale_offset_landsat(ds) is ds


# Spectral indices  - pure arithmetic, zero mocks

# Named constants so the expected-value expressions match the docstring formulas
# and are self-documenting when a test fails.
_NIR, _RED, _GREEN, _BLUE, _SWIR1, _SWIR2 = 0.8, 0.2, 0.4, 0.1, 0.5, 0.3


@pytest.fixture()
def spectral_ds():
    return _spectral_dataset(
        nir=_NIR,
        red=_RED,
        green=_GREEN,
        blue=_BLUE,
        swir16=_SWIR1,
        swir22=_SWIR2,
    )


@pytest.mark.parametrize(
    "num, den, expected",
    [(1.0, 2.0, 0.5), (np.nan, 2.0, np.nan), (1.0, np.nan, np.nan), (0.0, 0.0, np.nan), (1.0, 0.0, np.nan)],
    ids=["divides", "nan numerator", "nan denominator", "zero over zero", "nonzero over zero"],
)
def test_safe_divide(num, den, expected):
    """NaN if the denominator is 0 or either input is NaN, otherwise the quotient."""
    result = safe_divide(xr.DataArray(np.float32(num)), xr.DataArray(np.float32(den)))
    np.testing.assert_equal(result.item(), expected)
    assert result.dtype == np.float32


class TestCalculateIndices:
    """
    All spectral indices are ratios of scaled reflectance - pure arithmetic.
    Each index gets its own test so failures point to exactly one formula.
    """

    def test_ndvi(self, spectral_ds):
        expected = (_NIR - _RED) / (_NIR + _RED)
        assert calculate_indices(spectral_ds)["ndvi"].item() == pytest.approx(expected)

    def test_ndwi(self, spectral_ds):
        expected = (_GREEN - _NIR) / (_GREEN + _NIR)
        assert calculate_indices(spectral_ds)["ndwi"].item() == pytest.approx(expected)

    def test_mndwi(self, spectral_ds):
        expected = (_GREEN - _SWIR1) / (_GREEN + _SWIR1)
        assert calculate_indices(spectral_ds)["mndwi"].item() == pytest.approx(expected)

    def test_ndti(self, spectral_ds):
        expected = (_RED - _GREEN) / (_RED + _GREEN)
        assert calculate_indices(spectral_ds)["ndti"].item() == pytest.approx(expected)

    def test_bsi(self, spectral_ds):
        num = (_SWIR1 + _RED) - (_NIR + _BLUE)
        den = (_SWIR1 + _RED) + (_NIR + _BLUE)
        assert calculate_indices(spectral_ds)["bsi"].item() == pytest.approx(num / den)

    def test_mbi(self, spectral_ds):
        expected = (_SWIR1 - _SWIR2 - _NIR) / (_SWIR1 + _SWIR2 + _NIR) + 0.5
        assert calculate_indices(spectral_ds)["mbi"].item() == pytest.approx(expected)

    def test_baei(self, spectral_ds):
        expected = (_RED + 0.3) / (_GREEN + _SWIR1)
        assert calculate_indices(spectral_ds)["baei"].item() == pytest.approx(expected)

    def test_bui_is_ndbi_minus_ndvi(self, spectral_ds):
        ndbi = (_SWIR1 - _NIR) / (_SWIR1 + _NIR)
        ndvi = (_NIR - _RED) / (_NIR + _RED)
        assert calculate_indices(spectral_ds)["bui"].item() == pytest.approx(ndbi - ndvi)

    def test_ndbi_is_intermediate_only_not_stored(self, spectral_ds):
        assert "ndbi" not in calculate_indices(spectral_ds)

    def test_adds_indices_and_keeps_original_bands(self, spectral_ds):
        indices = {"ndvi", "ndwi", "mndwi", "ndti", "bsi", "mbi", "baei", "bui"}
        bands = {"nir08", "red", "green", "blue", "swir16", "swir22"}
        assert indices | bands <= set(calculate_indices(spectral_ds).data_vars)

    @pytest.mark.parametrize("index", ["ndvi", "ndwi", "mndwi", "ndti", "bsi", "mbi", "baei", "bui"])
    def test_all_zero_inputs_yield_nan(self, index):
        ds = _spectral_dataset(nir=0.0, red=0.0, green=0.0, blue=0.0, swir16=0.0, swir22=0.0)
        assert np.isnan(calculate_indices(ds)[index].item())

    def test_baei_zero_denominator_is_nan_not_inf(self):
        ds = _spectral_dataset(green=0.0, swir16=0.0)
        assert np.isnan(calculate_indices(ds)["baei"].item())

    def test_mutates_and_returns_same_dataset_object(self, spectral_ds):
        assert calculate_indices(spectral_ds) is spectral_ds

    @pytest.mark.parametrize("index", ["ndvi", "ndwi", "mndwi", "ndti", "bsi"])
    def test_normalised_differences_in_range(self, spectral_ds, index):
        assert -1.0 <= calculate_indices(spectral_ds)[index].item() <= 1.0

    @pytest.mark.parametrize("index", ["ndvi", "ndwi", "bsi", "mbi", "bui"])
    def test_nan_nir_propagates(self, index):
        ds = _spectral_dataset(nir=np.nan)
        assert np.isnan(calculate_indices(ds)[index].item())


# Terrain derivation


class TestComputeTerrain:
    """
    _calculate_terrain_features differentiates a geographic DEM and converts to ground metres.
    We verify the analytic cases we can reason about exactly:
    flat (slope = 0, no facing), ramps (slope = arctan(rise / run), facing away from the rise), the cos(latitude)
    shrinking of east-west distances, plus invariants that hold for any DEM.
    """

    def test_returns_elevation_slope_northness_and_eastness(self):
        assert set(_calculate_terrain_features(_flat_dem()).data_vars) == {
            "elevation",
            "slope",
            "northness",
            "eastness",
        }

    def test_flat_dem_has_zero_slope_northness_and_eastness(self):
        result = _calculate_terrain_features(_flat_dem(size=7, elevation=500.0))
        # Floating point coordinate spacing leaves ~1e-15 rounding, not exactly 0.
        for name in ["slope", "northness", "eastness"]:
            np.testing.assert_allclose(result[name].values, 0.0, atol=1e-9)

    def test_flat_dem_preserves_elevation_values(self):
        result = _calculate_terrain_features(_flat_dem(elevation=250.0))
        assert np.allclose(result["elevation"].values, 250.0)

    def test_terrain_rising_east_faces_west(self):
        result = _calculate_terrain_features(_x_ramp_dem())
        slope = np.arctan(1 / _pixel_m(0.0, east=True))
        assert result["slope"].values[1, 1] == pytest.approx(np.degrees(slope), rel=1e-4)
        assert result["eastness"].values[1, 1] == pytest.approx(-np.sin(slope), rel=1e-4)
        assert result["northness"].values[1, 1] == pytest.approx(0.0, abs=1e-6)

    def test_terrain_rising_north_faces_south(self):
        result = _calculate_terrain_features(_y_ramp_dem())
        slope = np.arctan(1 / _pixel_m(0.0, east=False))
        assert result["northness"].values[1, 1] == pytest.approx(-np.sin(slope), rel=1e-4)
        assert result["eastness"].values[1, 1] == pytest.approx(0.0, abs=1e-6)

    def test_row_order_does_not_change_the_result(self):
        north_up = _y_ramp_dem()
        south_up = north_up.isel(y=slice(None, None, -1))
        np.testing.assert_allclose(
            _calculate_terrain_features(south_up)["northness"].isel(y=slice(None, None, -1)).values,
            _calculate_terrain_features(north_up)["northness"].values,
        )

    def test_east_west_distances_shrink_with_latitude(self):
        """At 60 degrees a degree of longitude is about half as long, so an east-west ramp is about twice as steep."""
        steep = np.arctan(1 / _pixel_m(60.0, east=True))
        assert steep == pytest.approx(np.arctan(2 / _pixel_m(0.0, east=True)), rel=0.01)
        assert _calculate_terrain_features(_x_ramp_dem(lat=60.0))["slope"].values[1, 1] == pytest.approx(
            np.degrees(steep), rel=1e-4
        )
        # North-south distances change only slightly with latitude (ellipsoid flattening).
        expected = np.degrees(np.arctan(1 / _pixel_m(60.0, east=False)))
        assert _calculate_terrain_features(_y_ramp_dem(lat=60.0))["slope"].values[1, 1] == pytest.approx(
            expected, rel=1e-4
        )

    def test_northness_and_eastness_are_aspect_weighted_by_sin_slope_on_random_dem(self):
        dem = _geo_dem(np.random.default_rng(1).uniform(0, 500, (12, 12)), lat=-18.0)
        result = _calculate_terrain_features(dem)
        assert np.all(result["slope"].values >= 0)
        sin_slope = np.sin(np.radians(result["slope"]))
        np.testing.assert_allclose(result["northness"] ** 2 + result["eastness"] ** 2, sin_slope**2, rtol=1e-4)

    def test_output_spatial_coordinates_match_input(self):
        dem = _x_ramp_dem()
        result = _calculate_terrain_features(dem)
        np.testing.assert_array_equal(result["slope"].x.values, dem.x.values)
        np.testing.assert_array_equal(result["slope"].y.values, dem.y.values)


# load_across_antimeridian


def _am_geobox() -> GeoBox:
    """A small EPSG:3832 geobox centred on the antimeridian at 18S."""
    x_am, y_am = Transformer.from_crs("EPSG:4326", "EPSG:3832", always_xy=True).transform(180.0, -18.0)
    return GeoBox.from_bbox((x_am - 150, y_am - 150, x_am + 150, y_am + 150), "EPSG:3832", resolution=30)


def _item(west: float, east: float, crs: str) -> Item:
    """STAC item with a polygon footprint spanning west to east longitude, in a native CRS."""
    ring = [(west, -19.0), (east, -19.0), (east, -17.0), (west, -17.0), (west, -19.0)]
    geometry = {"type": "Polygon", "coordinates": [ring]}
    item = Item(
        id=f"{west}_{east}",
        geometry=geometry,
        bbox=[west, -19.0, east, -17.0],
        datetime=datetime(2020, 1, 1),
        properties={},
    )
    ProjectionExtension.add_to(item)
    ProjectionExtension.ext(item).code = crs
    return item


@pytest.mark.parametrize(
    "items",
    [[], [_item(179.0, 180.0, "EPSG:4326")], [_item(-180.0, -179.0, "EPSG:4326")]],
    ids=["no items", "east only", "west only"],
)
def test_load_across_antimeridian_raises_without_items_on_both_sides(items):
    with patch("ldn.raster.stac_load") as mock_load:
        with pytest.raises(LdnError, match="Expected to load 2 halves"):
            load_across_antimeridian(items, _am_geobox(), "bilinear")
    mock_load.assert_not_called()


def test_load_across_antimeridian_loads_projected_items_onto_the_geobox():
    """UTM footprints span -180 to 180 and UTM coordinates don't wrap, so they load directly without splitting."""
    items = [_item(-180.0, 180.0, "EPSG:32760"), _item(-180.0, 180.0, "EPSG:32701")]
    geobox = _am_geobox()
    with patch("ldn.raster.stac_load") as mock_load:
        load_across_antimeridian(items, geobox, "nearest")
    # Every item, unsplit, straight onto the geobox and lazy.
    mock_load.assert_called_once_with(items, geobox=geobox, chunks={}, resampling="nearest", patch_url=sign_url)


_STEP = 1 / 3600
_LATITUDE = -18.0 + np.arange(-8, 9) * _STEP


def _seam_half(longitude: np.ndarray, empty_last_column: bool) -> xr.Dataset:
    """One side of the antimeridian at 1 arcsecond, all 100 m, optionally with an empty last column."""
    data = np.full((len(_LATITUDE), len(longitude)), 100.0, dtype=np.float32)
    if empty_last_column:
        data[:, -1] = np.nan
    half = xr.Dataset(
        {"data": (("latitude", "longitude"), data)}, coords={"latitude": _LATITUDE, "longitude": longitude}
    )
    return assign_crs(half, "EPSG:4326")


# east ends with an empty column at 180. west starts at -180 with the float error odc gives (-179.99999999999997).
_EAST = _seam_half(180.0 - np.arange(8, -1, -1) * _STEP, empty_last_column=True)
_WEST = _seam_half(np.nextafter(-180.0, 0) + np.arange(9) * _STEP, empty_last_column=False)


def test_merge_geographic_halves_joins_the_180_columns_despite_float_error():
    """The two 180 columns become one, at 0, so the grid is evenly spaced with no empty column."""
    result = _merge_geographic_halves(_EAST, _WEST)
    np.testing.assert_allclose(np.diff(result.x.values), _STEP)
    assert result.x.size == 17
    assert result.x.values[8] == pytest.approx(0.0, abs=1e-9)
    np.testing.assert_array_equal(result["data"].values, 100.0)


def test_merge_geographic_halves_raises_for_halves_on_different_grids():
    """Halves shifted by half a pixel can't be joined by pixel position."""
    west = _seam_half(-180.0 + (np.arange(9) + 0.5) * _STEP, empty_last_column=False)
    with pytest.raises(LdnError, match="same pixel grid"):
        _merge_geographic_halves(_EAST, west)


def test_load_across_antimeridian_has_no_gap_where_both_halves_have_a_180_column():
    """The east half's empty column at 180 and the west half's -180 column must not leave a gap or duplicate."""
    geobox = _am_geobox()
    with patch("ldn.raster.stac_load") as mock_load:
        mock_load.return_value.squeeze.side_effect = [_EAST, _WEST]
        items = [_item(179.0, 180.0, "EPSG:4326"), _item(-180.0, -179.0, "EPSG:4326")]
        result = load_across_antimeridian(items, geobox, "bilinear")
    assert result["data"].shape == geobox.shape
    np.testing.assert_array_equal(result["data"].values, 100.0)


# load_dem_terrain guards
#

_MOD = "ldn.raster"


@pytest.fixture()
def mock_stac_client():
    with patch(f"{_MOD}.PyStacClient.open"):
        yield


def _stac_items(n: int) -> list:
    return [MagicMock() for _ in range(n)]


class TestLoadDemTerrainGuards:
    """Guard clauses only - no assertions about stac_load internals."""

    def test_raises_when_zero_items_found(self, mock_stac_client):
        with patch(f"{_MOD}.search_across_180", return_value=[]):
            with pytest.raises(LdnError, match="No DEM items found"):
                load_dem_terrain(MagicMock())

    @pytest.mark.parametrize("n", [10, 11, 100])
    def test_raises_when_ten_or_more_items_found(self, mock_stac_client, n):
        with patch(f"{_MOD}.search_across_180", return_value=_stac_items(n)):
            with pytest.raises(LdnError, match="Too many DEM items"):
                load_dem_terrain(MagicMock())


def _non_am_geobox() -> GeoBox:
    """A small EPSG:3832 geobox at 170E, 18S, away from the antimeridian."""
    x, y = Transformer.from_crs("EPSG:4326", "EPSG:3832", always_xy=True).transform(170.0, -18.0)
    return GeoBox.from_bbox((x - 150, y - 150, x + 150, y + 150), "EPSG:3832", resolution=30)


def _footprint_wgs84(geobox: GeoBox) -> GeoDataFrame:
    """A geobox's footprint in WGS84."""
    return GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs("EPSG:4326")


def _wgs84_dem(bbox: tuple[float, float, float, float], elevation: float) -> xr.Dataset:
    """Constant DEM on a 1 arcsecond WGS84 grid covering bbox, with a time dimension, as stac_load returns it."""
    geobox = GeoBox.from_bbox(bbox, "EPSG:4326", resolution=1 / 3600)
    return (xr_zeros(geobox, dtype="float32") + elevation).to_dataset(name="data").expand_dims(time=1)


class TestLoadDemTerrain:
    """load_dem_terrain on real geoboxes with a constant DEM. Only the STAC search and stac_load (network) are mocked,
    so the native grid load, the antimeridian join, terrain and reprojection run for real."""

    @staticmethod
    def _run(geobox: GeoBox, items: list, dems: list[xr.Dataset]) -> tuple[xr.Dataset, MagicMock]:
        """Run load_dem_terrain with stac_load returning dems in turn."""
        with (
            patch(f"{_MOD}.PyStacClient.open"),
            patch(f"{_MOD}.search_across_180", return_value=items),
            patch(f"{_MOD}.stac_load", side_effect=dems) as stac_load,
        ):
            return load_dem_terrain(geobox), stac_load

    @staticmethod
    def _assert_flat_terrain_on(result: xr.Dataset, geobox: GeoBox):
        """A constant 100 m DEM gives 100 m elevation and zero slope everywhere on the geobox."""
        assert set(result.data_vars) == {"elevation", "slope", "northness", "eastness"}
        assert result.odc.geobox.shape == geobox.shape and result.odc.crs == geobox.crs
        np.testing.assert_array_equal(result["elevation"].values, 100.0)
        # Floating point coordinate spacing leaves ~1e-14 degrees of slope, not exactly 0.
        np.testing.assert_allclose(result["slope"].values, 0.0, atol=1e-9)

    def test_tile_loads_native_grid_limited_to_the_padded_tile(self):
        geobox = _non_am_geobox()
        padded = tuple(_footprint_wgs84(geobox.pad(2)).total_bounds)
        result, stac_load = self._run(geobox, _stac_items(1), [_wgs84_dem(padded, 100.0)])
        kwargs = stac_load.call_args.kwargs
        assert kwargs["chunks"] == {} and "geobox" not in kwargs  # Native grid, lazy.
        assert kwargs["geopolygon"].union_all().contains(_footprint_wgs84(geobox).union_all())
        self._assert_flat_terrain_on(result, geobox)

    def test_antimeridian_tile_loads_and_joins_both_sides(self):
        geobox = _am_geobox()
        items = [_item(179.0, 180.0, "EPSG:4326"), _item(-180.0, -179.0, "EPSG:4326")]
        east, west = bbox_across_180(_footprint_wgs84(geobox.pad(2)))
        result, stac_load = self._run(geobox, items, [_wgs84_dem(east, 100.0), _wgs84_dem(west, 100.0)])
        assert stac_load.call_count == 2
        self._assert_flat_terrain_on(result, geobox)


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
_RASTER_MOD = "ldn.raster"

TILE = (66, 22)
YEAR = "2025"
VERSION = "test"
OWNER = "dep"


@pytest.fixture(autouse=False)
def mock_aws():
    """Mock all AWS I/O so no credentials are needed."""
    with patch(f"{_RASTER_MOD}.object_exists", return_value=False) as mock_object_exists:
        yield mock_object_exists


def test_build_pipeline_components_uses_public_url_base_for_full_path_prefix(mock_aws):
    """build_pipeline_components should call get_public_url_base(bucket) and pass its
    return value through to itempath.full_path_prefix — proves the wiring, not the URL logic."""
    with patch(f"{_RASTER_MOD}.get_public_url_base", return_value="https://example-base.com") as mock_get_url:
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
    with patch(f"{_RASTER_MOD}.object_exists", return_value=True):
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
    with patch(f"{_RASTER_MOD}.object_exists", return_value=True):
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
    add_processing_extension(item, ["ldn", "pystac"], providers)
    add_processing_extension(item, ["ldn", "pystac"], providers)  # Idempotent extension list.
    assert item.stac_extensions.count(PROCESSING_EXTENSION) == 1
    assert set(item.properties["processing:software"]) == {"ldn", "pystac"}
    assert item.properties["processing:version"] == installed_version("ldn")
    # RFC 3339 in UTC, as the extension requires.
    processed = datetime.fromisoformat(item.properties["processing:datetime"])
    assert item.properties["processing:datetime"].endswith("Z") and processed.utcoffset() == timedelta(0)
    assert [p.name for p in item.common_metadata.providers] == ["Digital Earth Pacific", "Auspatious", "USGS", "NASA"]
    auspatious = item.common_metadata.providers[1]
    assert auspatious.extra_fields["processing:facility"] == "Digital Earth Pacific"
    assert auspatious.extra_fields["processing:level"] == "L4"


def test_installed_version_appends_pinned_git_commit():
    """Git dependencies report the commit pinned in pyproject.toml, so STAC processing:software records it."""
    assert installed_version("numpy") == np.__version__
    sources = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())["tool"]["uv"]["sources"]
    for name in ["dep-tools", "datacube-compute"]:
        assert installed_version(name) == f"{version(name)}+{sources[name]['rev']}"
