from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from ldn.raster import (
    PrefixedS3ItemPath,
    _compute_terrain,
    _load_dem_am,
    calculate_indices,
    load_dem_terrain,
    scale_offset_landsat,
)
from ldn.utils import LdnError


def _band_dataset(**bands: np.ndarray) -> xr.Dataset:
    """Build a Dataset from keyword-name → 2-D uint16 array."""
    return xr.Dataset({name: (("y", "x"), arr.astype(np.uint16)) for name, arr in bands.items()})


def _spectral_dataset(nir=0.8, red=0.2, green=0.4, blue=0.1, swir16=0.5, swir22=0.3) -> xr.Dataset:
    """1×1 float Dataset with all bands required by calculate_indices."""
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


def _flat_dem(size=5, elevation=100.0, pixel_size=30.0) -> xr.DataArray:
    return xr.DataArray(
        np.full((size, size), elevation, dtype=np.float32),
        dims=("y", "x"),
        coords={
            "x": np.arange(size, dtype=float) * pixel_size,
            "y": np.arange(size, dtype=float) * pixel_size,
        },
        name="elevation",
    )


def _x_ramp_dem(pixel_size=30.0) -> xr.DataArray:
    """3×3 DEM rising by exactly 1 m per pixel eastward, flat north–south."""
    return xr.DataArray(
        np.array([[100, 101, 102], [100, 101, 102], [100, 101, 102]], dtype=np.float32),
        dims=("y", "x"),
        coords={
            "x": np.array([0, pixel_size, 2 * pixel_size]),
            "y": np.array([0, pixel_size, 2 * pixel_size]),
        },
        name="elevation",
    )


# Landsat scaling

SCALE = 0.0000275
OFFSET = -0.2


class TestScaleOffsetLandsat:
    """
    scale_offset_landsat is pure data transformation - no I/O, no mocks.
    We test the formula, both nodata sentinels, clipping, dtype, and that
    the non-spectral bands (count/emad/smad/bcmad) are left untouched.
    """

    def test_valid_pixel_applies_usgs_formula(self):
        raw = 10_000
        ds = _band_dataset(red=np.array([[raw]]))
        result = scale_offset_landsat(ds)
        expected = float(np.clip(raw * SCALE + OFFSET, 0, 1))
        assert result["red"].item() == pytest.approx(expected)

    @pytest.mark.parametrize("nodata", [0, 65_535])
    def test_nodata_sentinels_become_nan(self, nodata):
        ds = _band_dataset(red=np.array([[nodata]]))
        assert np.isnan(scale_offset_landsat(ds)["red"].item())

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
        ds = _band_dataset(red=np.array([[0, 10_000, 65_535]]))
        red = scale_offset_landsat(ds)["red"].values[0]
        assert np.isnan(red[0])
        assert red[1] == pytest.approx(float(np.clip(10_000 * SCALE + OFFSET, 0, 1)))
        assert np.isnan(red[2])

    def test_mutates_and_returns_same_dataset_object(self):
        ds = _band_dataset(red=np.array([[10_000]]))
        assert scale_offset_landsat(ds) is ds


# Spectral indices

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

    def test_all_output_bands_present(self, spectral_ds):
        result = calculate_indices(spectral_ds)
        assert {"ndvi", "ndwi", "mndwi", "ndti", "bsi", "mbi", "baei", "bui"}.issubset(result.data_vars)

    @pytest.mark.parametrize("index", ["ndvi", "ndwi", "mndwi", "ndti", "bsi"])
    def test_all_zero_inputs_yield_nan_for_ratio_indices(self, index):
        ds = _spectral_dataset(nir=0.0, red=0.0, green=0.0, blue=0.0, swir16=0.0, swir22=0.0)
        assert np.isnan(calculate_indices(ds)[index].item())

    def test_mutates_and_returns_same_dataset_object(self, spectral_ds):
        assert calculate_indices(spectral_ds) is spectral_ds


# Terrain derivation


class TestComputeTerrain:
    """
    _compute_terrain wraps scipy Sobel filters - pure computation.
    We verify the three analytic cases we can reason about exactly:
    flat (slope = 0), east-facing ramp (slope = arctan(1/res), aspect = 90°),
    plus invariants that hold for any DEM (non-negative slope, aspect in [0, 360)).
    """

    def test_returns_elevation_slope_and_aspect(self):
        assert set(_compute_terrain(_flat_dem()).data_vars) == {"elevation", "slope", "aspect"}

    def test_flat_dem_has_zero_slope_everywhere(self):
        result = _compute_terrain(_flat_dem(size=7, elevation=500.0))
        assert np.allclose(result["slope"].values, 0.0, atol=1e-5)

    def test_flat_dem_preserves_elevation_values(self):
        result = _compute_terrain(_flat_dem(elevation=250.0))
        assert np.allclose(result["elevation"].values, 250.0)

    def test_x_ramp_center_slope_matches_analytic_value(self):
        # Rise = 1 m per pixel, run = pixel_size → slope = arctan(1 / pixel_size).
        pixel_size = 30.0
        result = _compute_terrain(_x_ramp_dem(pixel_size))
        expected = np.degrees(np.arctan(1.0 / pixel_size))
        assert result["slope"].values[1, 1] == pytest.approx(expected, rel=1e-4)

    def test_x_ramp_center_aspect_points_east(self):
        # Terrain rising in +x (east) → aspect = 90°.
        result = _compute_terrain(_x_ramp_dem())
        assert result["aspect"].values[1, 1] == pytest.approx(90.0, abs=1.0)

    def test_slope_is_non_negative_on_random_dem(self):
        rng = np.random.default_rng(0)
        data = rng.uniform(0, 500, (12, 12)).astype(np.float32)
        dem = xr.DataArray(
            data,
            dims=("y", "x"),
            coords={"x": np.arange(12) * 30.0, "y": np.arange(12) * 30.0},
            name="elevation",
        )
        assert np.all(_compute_terrain(dem)["slope"].values >= 0)

    def test_aspect_is_within_0_to_360_on_random_dem(self):
        rng = np.random.default_rng(1)
        data = rng.uniform(0, 500, (12, 12)).astype(np.float32)
        dem = xr.DataArray(
            data,
            dims=("y", "x"),
            coords={"x": np.arange(12) * 30.0, "y": np.arange(12) * 30.0},
            name="elevation",
        )
        aspect = _compute_terrain(dem)["aspect"].values
        assert np.all(aspect >= 0) and np.all(aspect < 360)

    def test_output_spatial_coordinates_match_input(self):
        dem = _x_ramp_dem()
        result = _compute_terrain(dem)
        np.testing.assert_array_equal(result["slope"].x.values, dem.x.values)
        np.testing.assert_array_equal(result["slope"].y.values, dem.y.values)


# _load_dem_am
#
# _load_dem_am calls stac_load (odc-stac, HTTP) and rioxarray.reproject
# (GDAL).  We cannot run those without live network and data files in CI.
# What we *can* test without touching I/O is the guard clause: if the item
# lists produce fewer than two loadable halves, an LdnError must be raised
# before we ever touch the network.  We patch only stac_load and
# bbox_across_180; everything else is real code.
#
# If you want integration tests for the full antimeridian path, add a
# pytest mark (e.g. @pytest.mark.integration) and run against real tiles.


class TestLoadDemAmGuards:
    _PATCH = "ldn.raster"

    def _bbox_patch(self):
        return patch(
            f"{self._PATCH}.bbox_across_180",
            return_value=((170, -10, 180, 10), (-180, -10, -170, 10)),
        )

    def _east_item(self):
        item = MagicMock()
        item.bbox = [170, -10, 180, 10]  # min_lon ≥ 0 → east
        return item

    def _west_item(self):
        item = MagicMock()
        item.bbox = [-180, -10, -170, 10]  # min_lon < 0 → west
        return item

    @pytest.mark.parametrize(
        "items,label",
        [
            ([], "no items"),
            ([MagicMock(spec_set=["bbox"], bbox=[170, -10, 180, 10])], "east only"),
            ([MagicMock(spec_set=["bbox"], bbox=[-180, -10, -170, 10])], "west only"),
        ],
    )
    def test_raises_ldnerror_when_fewer_than_two_halves_loaded(self, items, label):
        """
        The function must raise before doing any I/O when the item list
        cannot produce two geographic halves.
        """
        geobox, geobox_wgs84 = MagicMock(), MagicMock()
        with self._bbox_patch(), patch(f"{self._PATCH}.stac_load") as mock_load:
            # Give stac_load a plausible-but-minimal return value so it
            # doesn't blow up if called - we only care about the guard.
            ds = xr.Dataset({"data": (("latitude", "longitude"), np.ones((2, 2)))})
            mock_load.return_value.squeeze.return_value = ds
            with pytest.raises(LdnError, match="Expected to load 2 halves"):
                _load_dem_am(items, geobox, geobox_wgs84)


# load_dem_terrain guards
#
# load_dem_terrain opens a STAC catalog over HTTP and calls odc-stac.
# Neither moto nor responses/httpretty cover the pystac-client protocol, so
# we mock PyStacClient.open and search_across_180.  We only test the guard
# clauses (item count bounds and antimeridian branching), because those are
# the only parts of this function that are *our* logic - the rest delegates
# to well-tested third-party libraries.


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

    @pytest.mark.parametrize("n", [1, 4, 9])
    def test_valid_item_counts_proceed_past_guard(self, mock_stac_client, n):
        """1–9 items must not trigger either guard clause."""
        geobox = MagicMock()
        geobox.crs = "EPSG:6933"
        geobox.extent.geom = MagicMock()

        elev_ds = xr.Dataset({"elevation": (("y", "x"), np.ones((4, 4), dtype=np.float32))})
        odc_ds = xr.Dataset({"elevation": (("y", "x"), np.ones((4, 4), dtype=np.float32))})
        odc_ds.odc = MagicMock()
        odc_ds.odc.assign_crs.return_value = elev_ds

        with (
            patch(f"{_MOD}.search_across_180", return_value=_stac_items(n)),
            patch(f"{_MOD}.GeoDataFrame") as mock_gdf,
            patch(f"{_MOD}.bbox_across_180", return_value=None),
            patch(f"{_MOD}.stac_load") as mock_stac_load,
            patch(f"{_MOD}._compute_terrain"),
        ):
            mock_gdf.return_value.to_crs.return_value = MagicMock()
            mock_stac_load.return_value.squeeze.return_value.rename.return_value = odc_ds
            load_dem_terrain(geobox)  # must not raise


class TestLoadDemTerrainBranching:
    """Antimeridian branching - verifies routing, not I/O results."""

    def _setup(self, crosses_am: bool):
        """Common fixture: patch everything except the branching logic."""
        bbox_return = ((170, -10, 180, 10), (-180, -10, -170, 10)) if crosses_am else None

        geobox = MagicMock()
        geobox.crs = "EPSG:3832" if crosses_am else "EPSG:6933"
        geobox.extent.geom = MagicMock()

        elev_ds = xr.Dataset({"elevation": (("y", "x"), np.ones((4, 4), dtype=np.float32))})
        odc_ds = xr.Dataset({"elevation": (("y", "x"), np.ones((4, 4), dtype=np.float32))})
        odc_ds.odc = MagicMock()
        odc_ds.odc.assign_crs.return_value = elev_ds

        return geobox, bbox_return, odc_ds

    def test_non_am_tile_uses_stac_load_not_load_dem_am(self):
        geobox, bbox_return, odc_ds = self._setup(crosses_am=False)
        with (
            patch(f"{_MOD}.PyStacClient.open"),
            patch(f"{_MOD}.search_across_180", return_value=_stac_items(1)),
            patch(f"{_MOD}.GeoDataFrame") as mock_gdf,
            patch(f"{_MOD}.bbox_across_180", return_value=bbox_return),
            patch(f"{_MOD}.stac_load") as mock_stac_load,
            patch(f"{_MOD}._load_dem_am") as mock_am,
            patch(f"{_MOD}._compute_terrain"),
        ):
            mock_gdf.return_value.to_crs.return_value = MagicMock()
            mock_stac_load.return_value.squeeze.return_value.rename.return_value = odc_ds
            load_dem_terrain(geobox)

        mock_stac_load.assert_called_once()
        mock_am.assert_not_called()

    def test_am_tile_uses_load_dem_am_not_stac_load(self):
        geobox, bbox_return, elev_ds = self._setup(crosses_am=True)
        am_ds = xr.Dataset({"elevation": (("y", "x"), np.ones((4, 4), dtype=np.float32))})
        am_ds.odc = MagicMock()
        am_ds.odc.assign_crs.return_value = elev_ds

        with (
            patch(f"{_MOD}.PyStacClient.open"),
            patch(f"{_MOD}.search_across_180", return_value=_stac_items(1)),
            patch(f"{_MOD}.GeoDataFrame") as mock_gdf,
            patch(f"{_MOD}.bbox_across_180", return_value=bbox_return),
            patch(f"{_MOD}._load_dem_am") as mock_am,
            patch(f"{_MOD}.stac_load") as mock_stac_load,
            patch(f"{_MOD}._compute_terrain"),
        ):
            mock_gdf.return_value.to_crs.return_value = MagicMock()
            mock_am.return_value = am_ds
            load_dem_terrain(geobox)

        mock_am.assert_called_once()
        mock_stac_load.assert_not_called()

    def test_stac_load_receives_chunks_empty_dict_for_lazy_loading(self):
        """Lazy-loading contract: chunks={} must be forwarded to stac_load."""
        geobox, _, odc_ds = self._setup(crosses_am=False)
        with (
            patch(f"{_MOD}.PyStacClient.open"),
            patch(f"{_MOD}.search_across_180", return_value=_stac_items(1)),
            patch(f"{_MOD}.GeoDataFrame") as mock_gdf,
            patch(f"{_MOD}.bbox_across_180", return_value=None),
            patch(f"{_MOD}.stac_load") as mock_stac_load,
            patch(f"{_MOD}._compute_terrain"),
        ):
            mock_gdf.return_value.to_crs.return_value = MagicMock()
            mock_stac_load.return_value.squeeze.return_value.rename.return_value = odc_ds
            load_dem_terrain(geobox)

        assert mock_stac_load.call_args.kwargs.get("chunks") == {}


# PrefixedS3ItemPath
#
# This class only builds path strings.  No boto3 client is created, so
# moto would add nothing here.  We test the path algebra directly.


@pytest.fixture()
def pather():
    """Default pather with a key_prefix but no full_path_prefix."""
    return PrefixedS3ItemPath(
        bucket="test-bucket",
        collection="test-collection",
        key_prefix="my-prefix",
    )


@pytest.fixture()
def pather_absolute():
    return PrefixedS3ItemPath(
        bucket="test-bucket",
        collection="test-collection",
        key_prefix="my-prefix",
        full_path_prefix="s3://test-bucket",
    )


class TestPrefixedS3ItemPath:
    """
    PrefixedS3ItemPath is pure string manipulation - no network, no boto3,
    no moto.  We test path structure and edge cases in isolation.
    """

    # --- relative paths ---

    def test_prefix_appears_at_start_of_relative_path(self, pather):
        assert pather.path("item-abc").startswith("my-prefix/")

    def test_item_id_is_in_relative_path(self, pather):
        assert "item-abc" in pather.path("item-abc")

    def test_default_extension_is_tif(self, pather):
        assert pather.path("item-abc").endswith(".tif")

    def test_custom_extension_is_honoured(self, pather):
        assert pather.path("item-abc", ext=".nc").endswith(".nc")

    def test_asset_name_appears_in_path_when_given(self, pather):
        assert "red" in pather.path("item-abc", asset_name="red")

    def test_no_double_slashes_in_relative_path(self, pather):
        assert "//" not in pather.path("item-abc")

    def test_relative_path_does_not_start_with_slash(self, pather):
        assert not pather.path("item-abc").startswith("/")

    @pytest.mark.parametrize("raw_prefix", ["/prefix/", " prefix ", "prefix/"])
    def test_prefix_slashes_are_normalised(self, raw_prefix):
        p = PrefixedS3ItemPath(bucket="b", collection="c", key_prefix=raw_prefix)
        path = p.path("item-x")
        assert not path.startswith("/")
        assert "//" not in path

    def test_no_prefix_gives_clean_relative_path(self):
        p = PrefixedS3ItemPath(bucket="b", collection="c")
        path = p.path("item-x")
        assert "item-x" in path
        assert not path.startswith("/")

    # --- absolute paths ---

    def test_absolute_path_starts_with_full_path_prefix(self, pather_absolute):
        assert pather_absolute.path("item-abc", absolute=True).startswith("s3://test-bucket")

    def test_absolute_path_contains_key_prefix(self, pather_absolute):
        assert "my-prefix" in pather_absolute.path("item-abc", absolute=True)

    def test_absolute_path_contains_item_id(self, pather_absolute):
        assert "item-abc" in pather_absolute.path("item-abc", absolute=True)

    def test_absolute_true_without_full_path_prefix_gives_relative(self, pather):
        """absolute=True is a no-op when full_path_prefix was not set."""
        result = pather.path("item-abc", absolute=True)
        assert not result.startswith("s3://")

    def test_absolute_path_has_no_double_slashes(self, pather_absolute):
        assert "//" not in pather_absolute.path("item-abc", absolute=True).replace("s3://", "")
