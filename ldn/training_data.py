"""Generate training data for LULC classification.

Loads 3 LULC products, finds agreement, generates stratified random samples,
extracts GeoMAD band values, filters outliers via K-Means clustering, and
uploads the result to S3.
"""

import io
import logging
from pathlib import Path
from typing import Annotated, Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray  # noqa: F401
import typer
import xarray as xr
from dep_tools.aws import object_exists
from dep_tools.utils import _fix_geometry, bbox_across_180, search_across_180
from odc.geo.geom import Geometry
from odc.geo.geom import box as odc_box
from odc.stac import load, stac_load
from planetary_computer import sign_url
from pystac import Item, ItemCollection
from pystac.client import Client
from rasterio.enums import Resampling
from rustac import search_sync
from scipy.ndimage import minimum_filter
from scipy.spatial.distance import cdist
from shapely.geometry import box
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from ldn.aws import s3_client
from ldn.grids import get_gadm, get_gridspec
from ldn.random_sampling import random_sampling
from ldn.raster import calculate_indices, load_dem_terrain, scale_offset_landsat
from ldn.typology import cci_lc_map, io_map, world_cover_map
from ldn.utils import (
    CLASS_ATTR,
    GEOMAD_DATASET_ID,
    GEOMAD_VERSION,
    SENSOR,
    TRAINING_DATA_VERSION,
    TRAINING_DATA_YEAR,
    WGS84,
    LdnError,
    dataset_prefix,
    get_analysis_epsg,
    get_env_var,
    get_stac_geoparquet_url,
    is_bucket_source_coop,
    owner_for_region,
    parse_tile_id,
)
from notebooks.src.Compare_LULC_func import standardise_class

logger = logging.getLogger(__name__)

cli_training_app = typer.Typer()

PC_CLIENT = None

product_nodata_value = 255

# These tiles are representative of different environments e.g. forest, atoll, volcanic, elevated, urban, beach,
# wetland, grassland, cropland, etc, Give me more if more than 5 are needed
PACIFIC_TRAINING_TILES = [
    # Papua New Guinea: Dense tropical rainforest & highland montane forest.
    ("028_030", "pacific", {"Papua New Guinea": "PNG"}),  # Capital city and coast.
    ("024_034", "pacific", {"Papua New Guinea": "PNG"}),  # Highland.
    ("023_031", "pacific", {"Papua New Guinea": "PNG"}),  # River delta.
    # Kiribati: Low-lying coral atoll, almost entirely at sea level, classic open-ocean/lagoon environment.
    ("058_043", "pacific", {"Kiribati": "KIR"}),
    ("059_040", "pacific", {"Kiribati": "KIR"}),
    # Vanuatu: Active volcanic islands with crater lakes, lava fields, and cloud forest.
    ("051_023", "pacific", {"Vanuatu": "VUT"}),
    ("053_018", "pacific", {"Vanuatu": "VUT"}),  # Mt Yasur volcano.
    ("052_022", "pacific", {"Vanuatu": "VUT"}),  # Lava lake.
    # Samoa: Elevated volcanic interior with waterfalls and lava tubes, fringed by reef/beach coastline.
    # 2 tiles pretty much covers all of Samoa.
    ("074_025", "pacific", {"Samoa": "WSM"}),
    ("075_025", "pacific", {"Samoa": "WSM"}),
    # Fiji: The most "mixed urban + agricultural" of the group, with sugarcane croplands,
    # mangrove wetlands, and a developed capital (Suva)
    ("063_020", "pacific", {"Fiji": "FJI"}),  # Elevation.
    ("066_022", "pacific", {"Fiji": "FJI"}),  # AM-crossing.
    ("064_020", "pacific", {"Fiji": "FJI"}),  # Suva urban area.
    # Palau for raised limestone/rock island jungle
    ("013_050", "pacific", {"Palau": "PLW"}),
    # New Caledonia for maquis shrubland / lagoon
    ("050_015", "pacific", {"New Caledonia": "NCL"}),
]
# PACIFIC_TRAINING_TILES are for training and validation.

# MODEL_TEST_TILES = [] # TODO: define this. It should have all classes! Maybe pick 2.
# Classify using a model (not trained on these). Compare output against LULC agreeing classes.


def _get_pc_client():
    """Return a cached Planetary Computer STAC client."""
    global PC_CLIENT
    if PC_CLIENT is None:
        PC_CLIENT = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1/")
    return PC_CLIENT


def _item_centroid_lon(item):
    """Return the WGS84 centroid longitude of a STAC item."""
    coords = item.geometry["coordinates"][0]
    return np.mean([c[0] for c in coords])


def _load_lulc_am(
    lulc_items: ItemCollection,
    geobox,
    geobox_wgs84: gpd.GeoDataFrame,
) -> xr.Dataset:
    """Load LULC product for a tile that crosses the antimeridian.

    Loads east and west halves separately. For geographic CRS products
    (WC, CCI in EPSG:4326), shifts west longitudes to >180 and
    reprojects with +over via rio.reproject. For projected CRS products
    (IO in UTM), reprojects each half to the target geobox independently
    via odc.reproject and merges non-zero values.

    Args:
        lulc_items: STAC items from search_across_180.
        geobox: Target geobox in the analysis CRS.
        geobox_wgs84: Tile footprint as a WGS84 GeoDataFrame.

    Returns:
        Dataset reprojected to the target geobox.
    """
    east_bbox, west_bbox = bbox_across_180(geobox_wgs84)
    east_gdf = gpd.GeoDataFrame(geometry=[box(*east_bbox)], crs=WGS84)
    west_gdf = gpd.GeoDataFrame(geometry=[box(*west_bbox)], crs=WGS84)

    east_items = [i for i in lulc_items if _item_centroid_lon(i) >= 0]
    west_items = [i for i in lulc_items if _item_centroid_lon(i) < 0]

    halves = []
    for items, gdf in [(east_items, east_gdf), (west_items, west_gdf)]:
        if not items:
            continue
        ds = load(items, geopolygon=gdf, chunks={}, patch_url=sign_url).squeeze(drop=True)
        halves.append(ds)

    if len(halves) != 2:
        raise ValueError(
            f"Expected 2 halves but got {len(halves)}. "
            f"East: {len(east_items)}, West: {len(west_items)}. "
            f"Check tile geometry and LULC item coverage."
        )

    is_geographic = "longitude" in halves[0].dims

    if not is_geographic:
        halves = [h.odc.reproject(geobox, resampling="nearest") for h in halves]
        merged = halves[0].copy()
        for var in merged.data_vars:
            merged[var] = xr.where(halves[1][var] != 0, halves[1][var], halves[0][var])
        return merged.odc.assign_crs(crs=geobox.crs)

    halves[1] = halves[1].assign_coords(longitude=(halves[1].longitude % 360))
    ds_combined = xr.concat(halves, dim="longitude").sortby("longitude")

    ds_combined = ds_combined.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    ds_combined = ds_combined.rio.write_crs("+proj=longlat +datum=WGS84 +over")

    return ds_combined.rio.reproject(
        str(geobox.crs),
        shape=(geobox.height, geobox.width),
        transform=geobox.transform,
        resampling=Resampling.nearest,
    )


def load_lulc_for_tile(product: str, geobox, year: str) -> xr.Dataset:
    """Search and load a LULC product onto a target geobox.

    Args:
        product: STAC collection ID (e.g. "esa-worldcover").
        geobox: Target geobox in the analysis CRS.
        year: Year string for temporal filtering.

    Returns:
        Dataset with LULC data aligned to the target geobox.
    """
    pc_client = _get_pc_client()

    lulc_items = search_across_180(
        geobox,
        pc_client,
        collections=[product],
        datetime=f"{year}-01-01/{year}-12-31",
    )
    if product == "io-lulc-annual-v02":
        lulc_items = ItemCollection([i for i in lulc_items if year in i.id])

    logger.info(f"Found {len(lulc_items)} {product} items")
    assert 0 < len(lulc_items) < 30

    geobox_wgs84 = gpd.GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(WGS84)
    crosses_am = isinstance(bbox_across_180(geobox_wgs84), tuple)

    if crosses_am:
        logger.info("AM-crossing tile, using split load")
        return _load_lulc_am(lulc_items, geobox, geobox_wgs84)

    logger.info("Non-AM-crossing tile, using normal load")
    return load(
        lulc_items,
        geobox=geobox,
        chunks={},
        resampling="nearest",
        patch_url=sign_url,
    ).squeeze(drop=True)


def _wc_quality_filter(ds):
    """ESA WorldCover quality filter.

    Retain pixels with at least 1 valid Sentinel observation in at
    least 2 of the 3 seasons. Where quality is nodata, do not count
    that season as failing.
    """
    q1 = ds["input_quality.1"]
    q2 = ds["input_quality.2"]
    q3 = ds["input_quality.3"]

    has_obs = (q1 > 0).astype(int) + (q2 > 0).astype(int) + (q3 > 0).astype(int)
    no_data = (q1 < 0).astype(int) + (q2 < 0).astype(int) + (q3 < 0).astype(int)

    return (has_obs >= 2) | (no_data == 3)


def _cci_quality_filter(ds):
    """ESA CCI-LC quality filter with fallback for low-observation tiles.

    Strict filter: processed, stable class, and at least 3 observations.
    Fallback: if strict rejects all pixels, relax to at least 1 observation.
    """
    processed = ds["processed_flag"] == 1
    stable = ds["change_count"] == 0
    obs = ds["observation_count"]

    strict = processed & stable & (obs >= 3)
    if strict.any():
        return strict

    relaxed = processed & stable & (obs >= 1)
    if relaxed.any():
        pct = float(relaxed.sum()) / relaxed.size * 100
        logger.info(f"CCI: strict filter (obs>=3) rejected all pixels, falling back to obs>=1 ({pct:.1f}% pass)")
        return relaxed

    logger.info("CCI: all quality filters rejected all pixels")
    return strict


LULC_PRODUCTS = [
    {
        "product": "esa-worldcover",
        "native_band": "map",
        "output_band": "esa_wc",
        "class_map": world_cover_map,
        "quality_fn": _wc_quality_filter,
        "quality_bands": ["input_quality.1", "input_quality.2", "input_quality.3"],
    },
    {
        "product": "esa-cci-lc",
        "native_band": "lccs_class",
        "output_band": "esa_cci",
        "class_map": cci_lc_map,
        "quality_fn": _cci_quality_filter,
        "quality_bands": ["processed_flag", "change_count", "observation_count"],
    },
    {
        "product": "io-lulc-annual-v02",
        "native_band": "data",
        "output_band": "io",
        "class_map": io_map,
        "quality_fn": None,
        "quality_bands": [],
    },
]


def load_and_prepare(
    geobox,
    country_wgs84_buffered: gpd.GeoDataFrame,
    product_dict: dict,
    year: str,
) -> xr.Dataset:
    """Load a LULC product, apply quality filtering, and standardise classes.

    Args:
        geobox: Target geobox for the tile.
        country_wgs84_buffered: Buffered country geometry in WGS84.
        product_dict: Product configuration dict from LULC_PRODUCTS.
        year: Year string for temporal filtering.

    Returns:
        xarray Dataset with standardised class band, clipped to the
        buffered country geometry..
    """
    product, native_band, output_band, class_map, quality_fn, quality_bands = product_dict.values()

    ds = load_lulc_for_tile(product, geobox, year)
    ds[output_band] = ds[native_band]

    bands_to_keep = {output_band} | set(quality_bands)
    ds = ds.drop_vars([v for v in ds.data_vars if v not in bands_to_keep])

    if "time" in ds.dims:
        ds = ds.isel(time=0)

    if quality_fn is not None:
        ds[output_band] = ds[output_band].where(quality_fn(ds))

    ds = ds.drop_vars([v for v in ds.data_vars if v != output_band])
    ds = ds.load()
    ds[output_band] = ds[output_band].astype("uint8")
    ds[output_band] = standardise_class(ds[output_band], class_map)

    country_prj = country_wgs84_buffered.to_crs(geobox.crs)
    clip_geom = Geometry(country_prj.union_all(), crs=geobox.crs)
    ds = ds.odc.crop(clip_geom, apply_mask=True, all_touched=True)

    for var in ds.data_vars:
        ds[var] = ds[var].fillna(product_nodata_value).astype("uint8")

    ds[output_band] = ds[output_band].where(ds[output_band] != 0, product_nodata_value)
    ds[output_band].attrs["nodata"] = product_nodata_value

    return ds


def find_agreement(wc: xr.Dataset, cci: xr.Dataset, io_ds: xr.Dataset) -> xr.DataArray:
    """Find 2-of-3 product agreement with neighbourhood filtering.

    Args:
        wc: WorldCover dataset with 'esa_wc' variable.
        cci: CCI-LC dataset with 'esa_cci' variable.
        io_ds: IO dataset with 'io' variable.

    Returns:
        DataArray of agreed class values.
    """

    def _clean(da):
        """Drop leftover scalar coordinates that cause merge conflicts."""
        drop = [c for c in da.coords if c not in da.dims]
        return da.drop_vars(drop)

    wc_da = _clean(wc["esa_wc"])
    cci_da = _clean(cci["esa_cci"])
    io_da = _clean(io_ds["io"])

    wc_ok = wc_da > 0
    cci_ok = cci_da > 0
    io_ok = io_da > 0
    has_data_count = wc_ok.astype("uint8") + cci_ok.astype("uint8") + io_ok.astype("uint8")
    valid = has_data_count >= 2

    wc_cci_agree = (wc_da == cci_da) & wc_ok & cci_ok
    cci_io_agree = (cci_da == io_da) & cci_ok & io_ok
    wc_io_agree = (wc_da == io_da) & wc_ok & io_ok

    pair_agree_count = wc_cci_agree.astype("uint8") + cci_io_agree.astype("uint8") + wc_io_agree.astype("uint8")

    two_of_three = (pair_agree_count >= 1) & valid
    majority_class = xr.where(wc_cci_agree, wc_da, xr.where(wc_io_agree, wc_da, io_da))

    neighbour_mask = xr.DataArray(
        minimum_filter(two_of_three.values.astype("float32"), size=3, mode="constant", cval=0) == 1,
        coords=two_of_three.coords,
        dims=two_of_three.dims,
    )

    agreed_class = majority_class.where(neighbour_mask & two_of_three, other=0).rename(CLASS_ATTR).astype("uint8")
    agreed_class = agreed_class.where(agreed_class != 0, product_nodata_value).astype("uint8")
    agreed_class.attrs["nodata"] = product_nodata_value

    return agreed_class


def generate_samples(
    agreed: xr.DataArray,
    geomad_dem_indices: xr.Dataset,
    n: int,
    min_sample_per_class_n: int,
) -> gpd.GeoDataFrame:
    """Generate stratified random samples from the agreement map.

    Args:
        agreed: Agreement classification DataArray.
        geomad_dem_indices: GeoMAD dataset (used to mask nodata areas).
        n: Total number of sample points.
        min_sample_per_class_n: Minimum samples per class.

    Returns:
        GeoDataFrame of sample points with class labels.
    """
    agree_computed = agreed.compute()

    geomad_valid = geomad_dem_indices["red"].notnull().compute().astype("uint8")
    geomad_valid_matched = geomad_valid.sel(x=agree_computed.x, y=agree_computed.y, method="nearest")
    geomad_valid_matched = geomad_valid_matched.assign_coords(x=agree_computed.x, y=agree_computed.y)

    agree_masked = agree_computed.where(geomad_valid_matched == 1, other=product_nodata_value)

    # Trim to bounding box of valid pixels
    valid_mask = agree_masked.values != product_nodata_value
    valid_rows = np.where(valid_mask.any(axis=1))[0]
    valid_cols = np.where(valid_mask.any(axis=0))[0]
    if len(valid_rows) == 0 or len(valid_cols) == 0:
        raise ValueError("No valid pixels found for sampling")

    agree_trimmed = agree_masked.isel(
        y=slice(valid_rows[0], valid_rows[-1] + 1),
        x=slice(valid_cols[0], valid_cols[-1] + 1),
    )
    logger.info(f"Trimmed grid: {agree_masked.shape} -> {agree_trimmed.shape}")

    agree_for_sampling = agree_trimmed.rename({"y": "latitude", "x": "longitude"})

    samples = random_sampling(
        da=agree_for_sampling,
        n=n,
        min_sample_n=min_sample_per_class_n,
    )
    logger.info(f"Generated {len(samples)} samples")
    return samples


def extract_geomad_dem_indices_values(
    samples: gpd.GeoDataFrame,
    geomad_dem_indices: xr.Dataset,
    analysis_crs: str,
) -> gpd.GeoDataFrame:
    """Extract GeoMAD band values at sample point locations.

    Args:
        samples: Sample points GeoDataFrame (WGS84).
        geomad_dem_indices: GeoMAD dataset with DEM and indices.
        analysis_crs: CRS of the GeoMAD dataset.

    Returns:
        GeoDataFrame with band values joined to sample points.
    """
    band_names = [v for v in geomad_dem_indices.data_vars if v != "spatial_ref"]
    samples_analysis = samples.to_crs(analysis_crs)

    xs = xr.DataArray(samples_analysis.geometry.x.values, dims="points")
    ys = xr.DataArray(samples_analysis.geometry.y.values, dims="points")

    sampled = geomad_dem_indices[band_names].sel(x=xs, y=ys, method="nearest").load()

    sampled_df = sampled.to_dataframe()[band_names].reset_index(drop=True)
    samples = samples.reset_index(drop=True).copy()
    samples = pd.concat([samples, sampled_df], axis=1)
    return samples


def remove_nan_samples(samples: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Remove samples with NaN values in feature columns.

    Args:
        samples: GeoDataFrame with band values.

    Returns:
        Cleaned GeoDataFrame with NaN rows removed.
    """
    exclude_cols = ["spatial_ref", "time", "lulc", "geometry"]
    feature_cols = [c for c in samples.columns if c not in exclude_cols]

    nan_counts = samples[feature_cols].isna().sum()
    nan_counts = nan_counts[nan_counts > 0]
    for col, count in nan_counts.items():
        percent_removed = 100 * count / len(samples)
        logger.info(f"NaN in {col}: {count} ({percent_removed:.1f}%)")

    nan_mask = samples[feature_cols].isna().any(axis=1)
    samples = samples[~nan_mask]
    samples = samples.drop(columns=["spatial_ref", "time"], errors="ignore")
    return samples


# Outliers can be important. Rare examples of valid members of a class. e.g. muddy water.
# Should we filter outliers?
# Is clustering within a class a good idea? 5 sub-classes for a class.
# Test this. Does it improve the model?
# Ablation study to test.
def filter_outliers(samples: gpd.GeoDataFrame, cap: float = 0.05) -> gpd.GeoDataFrame:
    """Filter outliers per class using K-Means clustering.

    For each class, finds optimal k via silhouette score, then removes
    the worst `cap` fraction of samples by distance from cluster centroid.

    Args:
        samples: GeoDataFrame with feature columns and 'lulc' column.
        cap: Maximum fraction of samples to remove per class.

    Returns:
        GeoDataFrame with outliers removed.
    """
    exclude_cols = ["lulc", "geometry", "time", "spatial_ref"]
    feature_cols = [c for c in samples.columns if c not in exclude_cols]
    samples["outlier"] = False

    valid_idx = samples.index
    X = samples.loc[valid_idx, feature_cols].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    classes = samples.loc[valid_idx, "lulc"].unique()

    for cls in classes:
        mask = (samples.loc[valid_idx, "lulc"] == cls).values
        idx = valid_idx[mask]
        Xc = X_scaled[mask]
        n = len(Xc)

        if n < 6:
            continue
        k_max = min(5, n // 5)
        if k_max < 2:
            continue

        best_k, best_score = 2, -1
        for k in range(2, k_max + 1):
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(Xc)
            score = silhouette_score(Xc, labels)
            if score > best_score:
                best_k, best_score = k, score

        km_final = KMeans(n_clusters=best_k, random_state=42, n_init=10).fit(Xc)
        labels = km_final.labels_
        centers = km_final.cluster_centers_

        dist_from_centroid = np.zeros(n)
        for cluster_id in range(best_k):
            in_cluster = labels == cluster_id
            pts = Xc[in_cluster]
            dists = cdist(pts, centers[cluster_id].reshape(1, -1)).flatten()
            dist_from_centroid[np.where(in_cluster)[0]] = dists

        n_flag = max(1, int(np.floor(n * cap)))
        worst_idx = np.argsort(dist_from_centroid)[-n_flag:]
        outlier_mask = np.zeros(n, dtype=bool)
        outlier_mask[worst_idx] = True

        samples.loc[idx[outlier_mask], "outlier"] = True

        logger.info(
            f"{str(cls):30s} | n={n:4d} | k={best_k} | sil={best_score:.3f} "
            f"| outliers={outlier_mask.sum():4d} ({100 * outlier_mask.mean():.1f}%)"
        )

    total = len(samples)
    outliers = samples["outlier"].sum()
    logger.info(f"Total: {total}, Clean: {total - outliers}, Outliers: {outliers}")

    samples = samples[~samples["outlier"]].drop(columns=["outlier"], errors="ignore")
    return samples


def _upload_dataframe_csv_to_s3(df, bucket: str, path: str) -> str:
    """Upload a dataframe as CSV to S3 and return the S3 URI.

    Args:
        df: DataFrame to upload.
        bucket: S3 bucket name.
        path: Key path within the bucket.

    Returns:
        S3 URI of the uploaded file.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    key = f"{path}"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    return f"s3://{bucket}/{key}"  # TODO: Use utils functions for S3 URI formatting.


# Dep tools utils have mask_to_gadm() which would be helpful, but I want to buffer gadm before masking.
def get_buffered_country(
    country_of_interest: dict[str, str],
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
) -> gpd.GeoDataFrame:
    """Fetch and buffer a country geometry for analysis (antimeridian-fixed).

    Retrieves country geometry from GADM, applies country-specific clipping for
    known edge cases (for example antimeridian handling for Fiji), buffers in
    the analysis CRS, and returns the result in WGS84.

    Args:
        country_of_interest: Mapping of country name to country code (single-item
            dictionary expected).
        analysis_crs: Projected CRS string used for buffering in meters.

    Returns:
        A GeoDataFrame containing buffered country geometry in `WGS84`.
    """
    buffer_m = 100

    country_gadm = get_gadm(countries=country_of_interest)

    country_gadm = gpd.GeoDataFrame(
        geometry=country_gadm.to_crs(analysis_crs).buffer(buffer_m).to_crs(WGS84),
        crs=WGS84,
    )
    # Do antimeridian fix. Needed for Fiji.
    rows = []
    for geom in country_gadm.geometry:
        fixed = _fix_geometry(geom)
        if fixed.geom_type == "MultiPolygon":
            rows.extend(fixed.geoms)  # one row per polygon (east/west of AM)
        else:
            rows.append(fixed)

    return gpd.GeoDataFrame(geometry=rows, crs=WGS84)


# get_tile_year_geomad_dem_indices uses a lot of the code in search_and_load_geomad_indices_dem,
# but the training data notebook needs the extra country clipping so they are separate functions.
def get_tile_year_geomad_dem_indices(
    tile_id: str,
    year: str,
    region: Literal["pacific", "non-pacific"],
    country_wgs84_buffered: gpd.GeoDataFrame,
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
    product_owner: str,
    bucket: str,
    geomad_version: str,
    single_region: bool,
    sensor: str,
) -> xr.Dataset:
    """Load GeoMAD + DEM features for a tile, clipped to buffered country.

    Delegates to search_and_load_geomad_indices_dem for the shared search/load/scale/
    indices/DEM logic, then clips to the intersection of the tile extent
    and the buffered country geometry.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Temporal filter used for GeoMAD item search (e.g. "2020").
        region: Grid region, either "pacific" or "non-pacific".
        country_wgs84_buffered: Buffered country geometry in WGS84.
        analysis_crs: Projected CRS string (e.g. "EPSG:3832").
        product_owner: Optional owner override (e.g. "dep" or "ci") for both regions.
        geomad_version: Optional GeoMAD version override.

    Returns:
        Dataset with GeoMAD bands, spectral indices, elevation, slope,
        and aspect, clipped to the tile-country intersection.
    """
    merged = search_and_load_geomad_indices_dem(
        tile_id=tile_id,
        year=year,
        region=region,
        analysis_crs=analysis_crs,
        geopolygon=country_wgs84_buffered,
        product_owner=product_owner,
        geomad_version=geomad_version,
        bucket=bucket,
        single_region=single_region,
        sensor=sensor,
    )

    # Clip to intersection of tile extent and buffered country
    country_prj = country_wgs84_buffered.to_crs(merged.odc.geobox.crs)
    tile_extent = merged.odc.geobox.extent
    country_union = country_prj.union_all()
    intersection = tile_extent.geom.intersection(country_union)
    clip_geom = Geometry(
        intersection,
        crs=merged.odc.geobox.crs,
    )
    merged = merged.odc.crop(clip_geom, apply_mask=True, all_touched=True)

    logger.info(f"Merged GeoMAD/DEM shape (after country clip): {merged.dims}")
    return merged


def make_training_data(
    tile_id: str,
    year: str,
    region: Literal["pacific", "non-pacific"],
    geomad_version: str,
    geomad_bucket: str,
    output_bucket: str,
    country_of_interest: dict[str, str],
    product_owner: str | None,
    file_prefix: str,
    n: int,
    min_sample_per_class_n: int,
    single_region: bool,
    sensor: str,
):
    """Generate training data for a single tile and upload to S3 as CSV.

    End-to-end pipeline: loads GeoMAD (from any bucket), loads 3 LULC products, finds
    agreement, samples, extracts band values, filters outliers, and
    writes results locally and to S3 (to any bucket, can be different from the GeoMAD bucket).
    We do not support writing training data to Source.Coop. We can if needed.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string (e.g. "2020").
        region: Either "pacific" or "non-pacific".
        training_data_version: Version string (e.g. "0-0-4").
        geomad_version: GeoMAD version string (e.g. "0-3-0").
        geomad_bucket: S3 bucket name for GeoMAD input.
        output_bucket: S3 bucket name for upload.
        country_of_interest: Dict mapping country name to ISO3 code.
            If None, uses all countries in the tile.
        n: Total number of sample points.
        min_sample_per_class_n: Minimum samples per class.
        product_owner: Optional override for the product owner.

    Returns:
        GeoDataFrame of final training samples.
    """
    logger.info("Getting buffered country geometry")
    analysis_crs = get_analysis_epsg(region)
    country_wgs84_buffered = get_buffered_country(country_of_interest, analysis_crs)

    # Clip country geometry to this tile's footprint before any data loading.
    # Critical for countries like Kiribati that span huge parts of the Pacific —
    # passing the full country geometry into get_tile_year_geomad_dem_indices
    # causes Dask to materialise a massive array, leading to OOM kills.
    # Skip for AM-crossing tiles: their WGS84 footprint straddles ±180° and
    # intersects incorrectly with standard WGS84 country geometries.
    logger.info("Clipping buffered country geometry to tile footprint")
    tile_id_tuple = parse_tile_id(tile_id)
    grid = get_gridspec(region=region)
    tile_geobox = grid.tile_geobox(tile_id_tuple)
    tile_footprint_wgs84 = gpd.GeoDataFrame(geometry=[tile_geobox.extent.geom], crs=tile_geobox.crs).to_crs(WGS84)
    tile_crosses_am = isinstance(bbox_across_180(tile_footprint_wgs84), tuple)
    if tile_crosses_am:
        logger.info("AM-crossing tile — skipping country clip to tile footprint")
    else:
        country_wgs84_buffered = gpd.GeoDataFrame(
            geometry=country_wgs84_buffered.intersection(tile_footprint_wgs84.union_all()),
            crs=WGS84,
        )
        country_wgs84_buffered = country_wgs84_buffered[
            country_wgs84_buffered.geometry.notna() & ~country_wgs84_buffered.is_empty
        ]
        if country_wgs84_buffered.empty:
            raise LdnError(f"Country geometry does not overlap tile {tile_id}")
        logger.info("Clipped country geometry to tile footprint")

    logger.info("Loading GeoMAD, DEM, and indices")
    owner = owner_for_region(region, product_owner)
    # TODO: Use build_prefix() here.
    geomad_dem_indices = get_tile_year_geomad_dem_indices(
        tile_id,
        year,
        region=region,
        country_wgs84_buffered=country_wgs84_buffered,
        analysis_crs=analysis_crs,
        product_owner=owner,
        bucket=geomad_bucket,
        geomad_version=geomad_version,
        single_region=single_region,
        sensor=sensor,
    )
    geobox = geomad_dem_indices.odc.geobox

    logger.info("Loading 3 LULC products")
    wc = load_and_prepare(geobox, country_wgs84_buffered, LULC_PRODUCTS[0], year)
    cci = load_and_prepare(geobox, country_wgs84_buffered, LULC_PRODUCTS[1], year)
    io_ds = load_and_prepare(geobox, country_wgs84_buffered, LULC_PRODUCTS[2], year)

    logger.info("Finding product agreement")
    agreed = find_agreement(wc, cci, io_ds)

    logger.info("Generating samples")
    samples = generate_samples(agreed, geomad_dem_indices, n=n, min_sample_per_class_n=min_sample_per_class_n)

    logger.info("Extracting GeoMAD values at sample points")
    samples = extract_geomad_dem_indices_values(samples, geomad_dem_indices, analysis_crs)

    logger.info("Removing NaN samples")
    samples = remove_nan_samples(samples)

    # TODO: Run a baseline version and then test if outlier filtering improves the model and classification.
    # logger.info("Filtering outliers")
    # samples = filter_outliers(samples)

    out_fname_local = f"ldn/{file_prefix}"
    Path(out_fname_local).parent.mkdir(parents=True, exist_ok=True)
    samples.to_file(f"{out_fname_local}.geojson", driver="GeoJSON", index=False)
    samples.to_csv(f"{out_fname_local}.csv", index=False)
    logger.info(f"Saved training data to {out_fname_local}")

    s3_uri = _upload_dataframe_csv_to_s3(samples, output_bucket, f"{file_prefix}.csv")
    logger.info(f"Uploaded training data to {s3_uri}")


@cli_training_app.command()
def generate_training_data(
    tile_id: str = typer.Option(..., help="Grid tile identifier (e.g. 058_043)"),
    year: str = typer.Option(TRAINING_DATA_YEAR, help=f"Year (e.g. {TRAINING_DATA_YEAR})"),
    region: Literal["pacific", "non-pacific"] = typer.Option(..., help="Region: pacific or non-pacific"),
    training_data_version: str = typer.Option(
        TRAINING_DATA_VERSION, help=f"Version (default: {TRAINING_DATA_VERSION})"
    ),
    geomad_version: str = typer.Option(GEOMAD_VERSION, help=f"Geomad version (default: {GEOMAD_VERSION})"),
    geomad_bucket: Annotated[
        str, typer.Option(help="S3 bucket for GeoMAD data. Defaults to Source.Coop.")
    ] = "us-west-2.opendata.source.coop",
    output_bucket: Annotated[
        str | None,
        typer.Option(
            help="S3 bucket for output data. Defaults to BUCKET env var which should be the Auspatious LDN bucket."
        ),
    ] = None,
    # TODO: Refactor so country data doesn't need to be passed. Not sure how.
    country_name: str = typer.Option(..., help="Country name (e.g. Fiji)"),
    country_code: str = typer.Option(..., help="Country ISO3 code (e.g. FJI)"),
    n: int = typer.Option(2100, help="Total number of sample points"),
    min_sample_per_class_n: int = typer.Option(300, help="Minimum samples per class"),
    overwrite: bool = typer.Option(False, help="Whether to overwrite existing data in S3"),
    product_owner: str | None = typer.Option(None, help="Override the default product owner"),
    single_region: bool = typer.Option(
        ...,
        help="Whether to use the single region prefix (e.g. 'dep_ls_geomad') "
        "or the generic prefix (e.g. 'ls_geomad') when accessing GeoMAD data.",
    ),
    sensor: str = typer.Option(help="Sensor name (e.g. 'ls')", default=SENSOR),
):
    """Generate training data for LULC classification.
    Read geomad from any bucket and write training data to any bucket.
    """
    output_bucket = output_bucket or get_env_var("BUCKET")  # Default
    if not output_bucket or not geomad_bucket:
        raise LdnError("Output bucket and GeoMAD bucket must be set.")
    country_of_interest = {country_name: country_code}

    logger.info(
        f"Creating training data for tile {tile_id}, year {year}, region {region}, geomad_bucket "
        f"{geomad_bucket}, output_bucket {output_bucket}, country {country_of_interest}, n={n}, "
        f"min_sample_per_class_n={min_sample_per_class_n}, overwrite={overwrite}, "
        f"product_owner={product_owner} training_data_version={training_data_version}, geomad_version={geomad_version},"
        f" sensor={sensor}"
    )
    if training_data_version != TRAINING_DATA_VERSION:
        logger.info(
            f"Overriding the latest LULC prediction version ({TRAINING_DATA_VERSION}) with "
            f"the specified version ({training_data_version})."
        )
    if geomad_version != GEOMAD_VERSION:
        logger.info(
            f"Overriding the latest GeoMAD version ({GEOMAD_VERSION}) with the specified version ({geomad_version})."
        )

    tile_id_x, tile_id_y = parse_tile_id(tile_id)

    # TODO: Does this exists check work with Source.Coop and normal S3 buckets?
    # TODO: I think it needs source coop prefix prefixed.
    # Zero padded indexes
    file_prefix = f"training_data/{training_data_version}/{region}/{tile_id_x:03d}/{tile_id_y:03d}/{year}/samples"
    # Training data shouldn't be written to source.coop, but supported just in case.
    _is_bucket_source_coop = is_bucket_source_coop(output_bucket)
    if _is_bucket_source_coop:
        raise NotImplementedError("Writing training data to Source.Coop is not supported.")
        # file_prefix = f"{SOURCE_COOP_PREFIX_LULC}/{file_prefix}"
    # logger.info(f"Checking if object exists at s3://{output_bucket}/{file_prefix}")

    if not overwrite:
        logger.info("Overwrite is False, checking for existing object")
        exists = object_exists(output_bucket, f"{file_prefix}.csv")
        if exists:
            logger.info("Item already exists and overwrite is False. Skipping.")
            return
        else:
            logger.info("Item does not exist, proceeding with processing.")
    else:
        logger.info("Overwrite is True, proceeding with processing.")

    make_training_data(
        tile_id,
        year,
        region,
        geomad_version,
        geomad_bucket,
        output_bucket,
        country_of_interest,
        product_owner,
        file_prefix,
        n,
        min_sample_per_class_n,
        single_region,
        sensor,
    )


def make_geomad_item_id(
    tile_id: str,
    sensor: str,
    year: str,
    product_owner: str,
) -> str:
    """Build the STAC item ID for a GeoMAD tile.

    Args:
        region: Either "pacific" or "non-pacific".
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string (e.g. "2020").
        product_owner: Owner (e.g. "dep" or "ci", or override) for the region.

    Returns:
        The full STAC item ID string.
    """
    prefix = dataset_prefix(product_owner, sensor, GEOMAD_DATASET_ID)
    return f"{prefix}_{tile_id}_{year}"


def search_and_load_geomad_indices_dem(
    tile_id: str,
    year: str,
    region: Literal["pacific", "non-pacific"],
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
    geopolygon: gpd.GeoDataFrame,
    product_owner: str,
    bucket: str,
    geomad_version: str,
    single_region: bool,
    sensor: str,
) -> xr.Dataset:
    """Search, load, scale, and merge GeoMAD bands, spectral indices, and DEM terrain for a tile.
        Supports antimeridian-crossing tiles.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string for the GeoMAD item search (e.g. "2020").
        region: Grid region, either "pacific" or "non-pacific".
        analysis_crs: The expected CRS of the GeoMAD data (either "EPSG:3832" or "EPSG:6933").
        geopolygon: GeoDataFrame used to constrain the stac_load extent (the country geom).
        product_owner: Owner (e.g. "dep" or "ci", or override).
        bucket: S3 bucket name where the GeoMAD data is stored.
        geomad_version: GeoMAD version string (e.g. "0-3-0").

    Returns:
        Merged dataset with GeoMAD bands, spectral indices, elevation,
        slope, and aspect, clipped to the tile proj:bbox.
    """
    # geomad_stac_geoparquet_key = get_stac_geoparquet_key(
    #     bucket, single_region, product_owner, sensor, "geomad", geomad_version
    # )
    geomad_stac_geoparquet_key = ""  # TODO: Fix this.
    geomad_stac_geoparquet_url = get_stac_geoparquet_url(bucket, geomad_stac_geoparquet_key)
    item_id = make_geomad_item_id(tile_id, sensor, year, product_owner=product_owner)

    logger.info(f"Searching for GeoMAD item for tile {tile_id} and year {year}.")
    if GEOMAD_VERSION != geomad_version:
        logger.info(f"Using overridden GeoMAD version {geomad_version} instead of default {GEOMAD_VERSION}")
    else:
        logger.info(f"Using latest GeoMAD version {GEOMAD_VERSION}")

    geomad_items = search_sync(
        geomad_stac_geoparquet_url,
        ids=item_id,
    )
    geomad_items = [Item.from_dict(doc) for doc in geomad_items]
    geomad_items_n = len(geomad_items)
    logger.info(f"Found {geomad_items_n} GeoMAD items for tile {tile_id} and year {year}")

    if geomad_items_n != 1:
        raise LdnError(f"Must find exactly 1 GeoMAD item for this tile and year, found {geomad_items_n} instead.")

    proj_bbox = geomad_items[0].properties.get("proj:bbox")
    if proj_bbox is None:
        raise LdnError("GeoMAD item is missing 'proj:bbox' property.")
    logger.info(f"proj:bbox = {proj_bbox}")

    bands = [b for b in geomad_items[0].assets.keys() if b != "count"]
    logger.info(f"Loading bands: {bands}")

    geomad_ds = stac_load(
        geomad_items,
        chunks={},  # Force lazy.
        bands=bands,
        fail_on_error=True,  # We control the data so it shouldn't fail.
        geopolygon=geopolygon,
    )

    if geomad_ds.odc.crs.epsg != int(analysis_crs.split(":")[1]):
        raise LdnError(
            f"GeoMAD dataset CRS (EPSG:{geomad_ds.odc.crs.epsg}) does not match analysis CRS ({analysis_crs})"
        )
    logger.info(f"GeoMAD CRS: EPSG:{geomad_ds.odc.crs.epsg}")
    logger.info(f"GeoMAD shape: {geomad_ds.dims}")

    geomad_ds = geomad_ds.squeeze()

    # Clip to tile proj:bbox (the dataset may span the full country extent)
    tile_geom = odc_box(
        proj_bbox[0],
        proj_bbox[1],
        proj_bbox[2],
        proj_bbox[3],
        crs=analysis_crs,
    )
    # apply_mask not needed for this box crop.
    geomad_ds = geomad_ds.odc.crop(tile_geom, apply_mask=False)
    logger.info(f"GeoMAD shape (after tile clip): {geomad_ds.dims}")

    geomad_ds = scale_offset_landsat(geomad_ds)
    geomad_ds = calculate_indices(geomad_ds)

    dem_ds = load_dem_terrain(geomad_ds.odc.geobox)

    # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
    # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
    if "spatial_ref" in dem_ds.coords:
        dem_ds = dem_ds.drop_vars("spatial_ref")

    # Fix: assign GeoMAD coords to DEM before merge
    dem_ds = dem_ds.assign_coords(x=geomad_ds.x, y=geomad_ds.y)
    merged = xr.merge([geomad_ds, dem_ds], join="override")  # Override prefers geomad
    logger.info(f"Merged GeoMAD+DEM shape: {merged.dims}")
    return merged
