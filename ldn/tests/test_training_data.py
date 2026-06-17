from unittest.mock import MagicMock

import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from shapely.geometry import Point

from ldn.training_data import (
    _cci_quality_filter,
    _item_centroid_lon,
    _wc_quality_filter,
    extract_geomad_dem_indices_values,
    filter_outliers,
    find_agreement,
    make_geomad_item_id,
    remove_nan_samples,
)


class TestItemCentroidLon:
    def test_simple_polygon(self):
        """Returns mean longitude of item geometry coordinates."""
        item = MagicMock()
        item.geometry = {
            "coordinates": [
                [
                    (170.0, -10.0),
                    (172.0, -10.0),
                    (172.0, -12.0),
                    (170.0, -12.0),
                    (170.0, -10.0),
                ]
            ]
        }
        assert _item_centroid_lon(item) == pytest.approx(170.8)

    def test_negative_longitude(self):
        """Handles western hemisphere coordinates."""
        item = MagicMock()
        item.geometry = {
            "coordinates": [
                [
                    (-170.0, -10.0),
                    (-168.0, -10.0),
                    (-168.0, -12.0),
                    (-170.0, -12.0),
                    (-170.0, -10.0),
                ]
            ]
        }
        assert _item_centroid_lon(item) < 0


class TestWcQualityFilter:
    def _make_ds(self, q1_vals, q2_vals, q3_vals):
        """Helper to create a dataset with quality bands."""
        return xr.Dataset(
            {
                "input_quality.1": xr.DataArray(q1_vals, dims="x"),
                "input_quality.2": xr.DataArray(q2_vals, dims="x"),
                "input_quality.3": xr.DataArray(q3_vals, dims="x"),
            }
        )

    def test_all_seasons_valid(self):
        """Pixel passes when all 3 seasons have observations."""
        ds = self._make_ds([5, 5], [3, 3], [2, 2])
        result = _wc_quality_filter(ds)
        assert result.all()

    def test_two_seasons_valid(self):
        """Pixel passes with 2 valid seasons."""
        ds = self._make_ds([5], [0], [2])
        result = _wc_quality_filter(ds)
        assert result.item()

    def test_one_season_valid_fails(self):
        """Pixel fails with only 1 valid season."""
        ds = self._make_ds([5], [0], [0])
        result = _wc_quality_filter(ds)
        assert not result.item()

    def test_all_nodata_passes(self):
        """Pixel passes when all quality bands are nodata (negative)."""
        ds = self._make_ds([-1], [-1], [-1])
        result = _wc_quality_filter(ds)
        assert result.item()


class TestCciQualityFilter:
    def _make_ds(self, processed, change_count, obs_count):
        """Helper to create a CCI quality dataset."""
        return xr.Dataset(
            {
                "processed_flag": xr.DataArray(processed, dims="x"),
                "change_count": xr.DataArray(change_count, dims="x"),
                "observation_count": xr.DataArray(obs_count, dims="x"),
            }
        )

    def test_strict_passes(self):
        """Pixel passes strict filter: processed, stable, obs>=3."""
        ds = self._make_ds([1], [0], [5])
        result = _cci_quality_filter(ds)
        assert result.item()

    def test_strict_fails_low_obs(self):
        """Pixel fails strict filter with obs<3 but passes relaxed."""
        ds = self._make_ds([1], [0], [1])
        result = _cci_quality_filter(ds)
        assert result.item()  # Falls back to relaxed

    def test_not_processed_fails(self):
        """Pixel fails when not processed."""
        ds = self._make_ds([0], [0], [5])
        result = _cci_quality_filter(ds)
        assert not result.item()

    def test_unstable_fails(self):
        """Pixel fails when change_count > 0."""
        ds = self._make_ds([1], [1], [5])
        result = _cci_quality_filter(ds)
        assert not result.item()


class TestFindAgreement:
    def _make_lulc_ds(self, band_name, values):
        """Create a single-band LULC dataset."""
        return xr.Dataset(
            {
                band_name: xr.DataArray(
                    values,
                    dims=("y", "x"),
                    coords={
                        "y": np.arange(values.shape[0]),
                        "x": np.arange(values.shape[1]),
                    },
                )
            }
        )

    def test_all_agree(self):
        """All three products agree on class 1."""
        vals = np.full((5, 5), 1, dtype="uint8")
        wc = self._make_lulc_ds("esa_wc", vals)
        cci = self._make_lulc_ds("esa_cci", vals)
        io = self._make_lulc_ds("io", vals)

        result = find_agreement(wc, cci, io)
        # Centre pixels (not edges) should have agreement
        assert result.values[2, 2] == 1

    def test_no_agreement(self):
        """No two products agree, result should be nodata (255)."""
        wc = self._make_lulc_ds("esa_wc", np.full((5, 5), 1, dtype="uint8"))
        cci = self._make_lulc_ds("esa_cci", np.full((5, 5), 2, dtype="uint8"))
        io = self._make_lulc_ds("io", np.full((5, 5), 3, dtype="uint8"))

        result = find_agreement(wc, cci, io)
        assert (result.values == 255).all()

    def test_two_of_three_agree(self):
        """Two products agree, one disagrees."""
        wc = self._make_lulc_ds("esa_wc", np.full((5, 5), 6, dtype="uint8"))
        cci = self._make_lulc_ds("esa_cci", np.full((5, 5), 6, dtype="uint8"))
        io = self._make_lulc_ds("io", np.full((5, 5), 2, dtype="uint8"))

        result = find_agreement(wc, cci, io)
        assert result.values[2, 2] == 6

    def test_nodata_excluded(self):
        """Products with value 0 (nodata) don't count."""
        wc = self._make_lulc_ds("esa_wc", np.full((5, 5), 1, dtype="uint8"))
        cci = self._make_lulc_ds("esa_cci", np.full((5, 5), 0, dtype="uint8"))
        io = self._make_lulc_ds("io", np.full((5, 5), 1, dtype="uint8"))

        result = find_agreement(wc, cci, io)
        # wc and io agree on class 1
        assert result.values[2, 2] == 1


class TestRemoveNanSamples:
    def test_removes_rows_with_nan(self):
        """Rows with NaN in feature columns are removed."""
        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1, 2, 3],
                "red": [0.1, np.nan, 0.3],
                "green": [0.2, 0.2, 0.3],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            }
        )
        result = remove_nan_samples(gdf)
        assert len(result) == 2
        assert 1 not in result.index  # row with NaN removed

    def test_no_nans_unchanged(self):
        """All rows kept when no NaN values."""
        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1, 2],
                "red": [0.1, 0.2],
                "geometry": [Point(0, 0), Point(1, 1)],
            }
        )
        result = remove_nan_samples(gdf)
        assert len(result) == 2

    def test_drops_spatial_ref_and_time(self):
        """Columns spatial_ref and time are dropped if present."""
        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1],
                "red": [0.1],
                "spatial_ref": [0],
                "time": ["2020"],
                "geometry": [Point(0, 0)],
            }
        )
        result = remove_nan_samples(gdf)
        assert "spatial_ref" not in result.columns
        assert "time" not in result.columns


class TestFilterOutliers:
    def test_removes_some_outliers(self):
        """Outliers are removed (up to 5% cap per class)."""
        np.random.seed(42)
        n = 100
        # Create cluster with one obvious outlier
        red = np.concatenate([np.random.normal(0.5, 0.01, n - 1), [10.0]])
        green = np.concatenate([np.random.normal(0.3, 0.01, n - 1), [10.0]])

        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1] * n,
                "red": red,
                "green": green,
                "geometry": [Point(i, i) for i in range(n)],
            }
        )
        result = filter_outliers(gdf)
        assert len(result) < n

    def test_small_class_skipped(self):
        """Classes with fewer than 6 samples are not filtered."""
        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1, 1, 1, 1, 1],
                "red": [0.1, 0.2, 0.3, 0.4, 100.0],
                "green": [0.1, 0.2, 0.3, 0.4, 100.0],
                "geometry": [Point(i, i) for i in range(5)],
            }
        )
        result = filter_outliers(gdf)
        assert len(result) == 5

    def test_cap_limits_removal(self):
        """At most 5% of samples per class are removed."""
        np.random.seed(42)
        n = 200
        gdf = gpd.GeoDataFrame(
            {
                "lulc": [1] * n,
                "red": np.random.normal(0.5, 0.1, n),
                "green": np.random.normal(0.3, 0.1, n),
                "geometry": [Point(i, i) for i in range(n)],
            }
        )
        result = filter_outliers(gdf)
        removed = n - len(result)
        assert removed <= int(np.floor(n * 0.05))


class TestExtractGeomadDemIndicesValues:
    def test_extracts_band_values(self):
        """Band values are correctly extracted at sample locations."""
        x = np.arange(0, 100, 10, dtype=float)
        y = np.arange(0, 50, 10, dtype=float)
        red = np.arange(50, dtype=float).reshape(5, 10)

        ds = xr.Dataset(
            {
                "red": xr.DataArray(red, dims=("y", "x"), coords={"y": y, "x": x}),
            }
        )
        ds = ds.rio.write_crs("EPSG:3832")

        # Sample at pixel centres
        samples = gpd.GeoDataFrame(
            {
                "lulc": [1, 2],
                "geometry": [Point(0, 0), Point(50, 20)],
            },
            crs="EPSG:3832",
        )

        result = extract_geomad_dem_indices_values(samples, ds, "EPSG:3832")
        assert "red" in result.columns
        assert len(result) == 2
        # Point (0, 0) -> y_idx=0, x_idx=0 -> red[0,0] = 0
        assert result.iloc[0]["red"] == 0.0
        # Point (50, 20) -> y_idx=2, x_idx=5 -> red[2,5] = 25
        assert result.iloc[1]["red"] == 25.0


class TestGetGeomadItemId:
    def test_pacific(self):
        item_id = make_geomad_item_id("058_043", "2020", "dep")
        assert item_id == "dep_ls_geomad_058_043_2020"

    def test_non_pacific(self):
        item_id = make_geomad_item_id("119_126", "2023", "ci")
        assert item_id == "ci_ls_geomad_119_126_2023"

    def test_product_owner_override(self):
        item_id = make_geomad_item_id("058_043", "2020", "ci")
        assert item_id == "ci_ls_geomad_058_043_2020"
