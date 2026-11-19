"""Generate training data for LULC classification.

Loads the LULC products with data for the year, finds where at least 2 agree, generates stratified random samples,
extracts GeoMAD band values, filters outliers via K-Means clustering, and
uploads the result to S3.
"""

import io
import logging
from itertools import combinations
from pathlib import Path
from typing import Annotated, Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import typer
import xarray as xr
from antimeridian import fix_multi_polygon, fix_polygon
from dep_tools.aws import object_exists
from dep_tools.utils import bbox_across_180, search_across_180
from odc.geo.geom import Geometry
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import ItemCollection
from pystac.client import Client
from scipy.ndimage import minimum_filter
from scipy.spatial.distance import cdist
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from ldn.aws import s3_client
from ldn.grids import get_gadm, get_gridspec
from ldn.lulc import GeomadSearcher
from ldn.random_sampling import random_sampling
from ldn.raster import FOCAL_FEATURES, FOCAL_PAD, crop_to_geobox, load_across_antimeridian, make_features
from ldn.typology import cci_lc_map, io_map, world_cover_map
from ldn.utils import (
    CLASS_ATTR,
    GEOMAD_VERSION,
    LULC_DATASET_ID,
    LULC_NODATA,
    SENSOR,
    TEST_DATA_VERSION,
    TRAINING_DATA_VERSION,
    WGS84,
    LdnError,
    dataset_prefix,
    get_analysis_epsg,
    get_env_var,
    get_public_url_base,
    is_bucket_source_coop,
    owner_for_region,
    parse_tile_id,
    standardise_class,
)

logger = logging.getLogger(__name__)

cli_training_app = typer.Typer()

PC_CLIENT = None

PACIFIC_TRAINING_TILES = [
    ("013_050", "pacific", {"Palau": "PLW"}),  # 1, 2, 4, 5, 6
    ("023_031", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 4, 5, 6
    ("024_034", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 3, 5, 6, 7
    ("024_037", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 3, 4, 6
    ("025_057", "pacific", {"Guam": "GUM"}),  # 1, 2, 5, 6
    ("028_030", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 3, 4, 5, 6, 7
    ("034_036", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 4, 6, 7
    ("039_033", "pacific", {"Solomon Islands": "SLB"}),  # 1, 2, 4, 6
    ("040_031", "pacific", {"Solomon Islands": "SLB"}),  # 1, 2, 4, 6
    ("043_030", "pacific", {"Solomon Islands": "SLB"}),  # 1, 2, 3, 4, 6, 7
    ("049_029", "pacific", {"Solomon Islands": "SLB"}),  # 1, 4, 6
    ("050_015", "pacific", {"New Caledonia": "NCL"}),  # 1, 2, 3, 4, 5, 6, 7
    ("052_022", "pacific", {"Vanuatu": "VUT"}),  # 1, 2, 6, 7
    ("053_018", "pacific", {"Vanuatu": "VUT"}),  # 1, 2, 6, 7
    ("061_040", "pacific", {"Kiribati": "KIR"}),  # 1, 2, 6, 7
    ("064_020", "pacific", {"Fiji": "FJI"}),  # 1, 2, 3, 4, 5, 6
    ("071_018", "pacific", {"Tonga": "TON"}),  # 1, 2, 6, 7
    ("075_025", "pacific", {"Samoa": "WSM"}),  # 1, 2, 4, 5, 6
    ("101_020", "pacific", {"French Polynesia": "PYF"}),  # 1, 2, 3, 5, 6
    ("125_012", "pacific", {"Pitcairn Islands": "PCN"}),  # 1, 2, 6
]
NON_PACIFIC_TRAINING_TILES = [
    ("119_126", "non-pacific", {"Belize": "BLZ"}),  # 1, 2, 3, 4, 5, 6, 7
    ("124_133", "non-pacific", {"Cuba": "CUB"}),  # 1, 2, 3, 4, 5, 6
    ("126_129", "non-pacific", {"Cayman Islands": "CYM"}),  # 1, 2, 4, 5, 6
    ("126_133", "non-pacific", {"Cuba": "CUB"}),  # 1, 2, 3, 4, 5, 6
    ("129_135", "non-pacific", {"Bahamas": "BHS"}),  # 1, 2, 4, 6, 7
    ("129_138", "non-pacific", {"Bahamas": "BHS"}),  # 1, 2, 4, 5, 6, 7
    ("130_127", "non-pacific", {"Jamaica": "JAM"}),  # 1, 2, 3, 4, 5, 6, 7
    ("131_131", "non-pacific", {"Cuba": "CUB"}),  # 1, 2, 3, 4, 5, 6, 7
    ("135_129", "non-pacific", {"Haiti": "HTI"}),  # 1, 2, 3, 4, 5, 6, 7
    ("136_127", "non-pacific", {"Dominican Republic": "DOM"}),  # 1, 2, 3, 4, 5, 6, 7
    ("146_118", "non-pacific", {"Trinidad and Tobago": "TTO"}),  # 1, 2, 3, 4, 5, 6, 7
    ("147_123", "non-pacific", {"Martinique": "MTQ"}),  # 1, 2, 3, 4, 5, 6
    ("148_109", "non-pacific", {"Guyana": "GUY"}),  # 1, 2, 4, 6
    ("149_113", "non-pacific", {"Guyana": "GUY"}),  # 1, 2, 3, 4, 5, 6
    ("153_109", "non-pacific", {"Suriname": "SUR"}),  # 1, 2, 4, 6, 7
    ("183_126", "non-pacific", {"Cabo Verde": "CPV"}),  # 1, 2, 5, 6, 7
    ("194_120", "non-pacific", {"Guinea-Bissau": "GNB"}),  # 1, 2, 3, 5, 6
    ("251_088", "non-pacific", {"Comoros": "COM"}),  # 1, 2, 5, 6, 7
    ("266_077", "non-pacific", {"Mauritius": "MUS"}),  # 1, 2, 3, 4, 5, 6
    ("312_105", "non-pacific", {"Singapore": "SGP"}),  # 1, 2, 3, 4, 5, 6, 7
]
TRAINING_TILES = PACIFIC_TRAINING_TILES + NON_PACIFIC_TRAINING_TILES

PACIFIC_TEST_TILES = [
    ("017_052", "pacific", {"Micronesia": "FSM"}),  # 1, 2, 4, 5, 6
    ("020_034", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 4, 6, 7
    ("026_059", "pacific", {"Northern Mariana Islands": "MNP"}),  # 1, 2, 3, 5, 6
    ("026_034", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 3, 4, 6, 7
    ("030_030", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 4, 6
    ("032_035", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 3, 4, 6, 7
    ("037_034", "pacific", {"Papua New Guinea": "PNG"}),  # 1, 2, 4, 6, 7
    ("040_049", "pacific", {"Micronesia": "FSM"}),  # 1, 4, 6
    ("042_032", "pacific", {"Solomon Islands": "SLB"}),  # 1, 4, 6
    ("045_029", "pacific", {"Solomon Islands": "SLB"}),  # 1, 2, 4, 6
    ("048_017", "pacific", {"New Caledonia": "NCL"}),  # 1, 2, 3, 4, 6, 7
    ("050_023", "pacific", {"Vanuatu": "VUT"}),  # 1, 2, 6
    ("051_025", "pacific", {"Vanuatu": "VUT"}),  # 1, 2, 6
    ("052_020", "pacific", {"Vanuatu": "VUT"}),  # 1, 2, 3, 6
    ("064_022", "pacific", {"Fiji": "FJI"}),  # 1, 2, 3, 4, 6
    ("070_026", "pacific", {"Wallis and Futuna": "WLF"}),  # 1, 2, 6
    ("071_016", "pacific", {"Tonga": "TON"}),  # 1, 2, 3, 4, 6
    ("073_019", "pacific", {"Tonga": "TON"}),  # 1, 2, 6
    ("077_019", "pacific", {"Niue": "NIU"}),  # 1, 2, 6
    ("092_043", "pacific", {"Kiribati": "KIR"}),  # 2, 6, 7
]
NON_PACIFIC_TEST_TILES = [
    ("119_128", "non-pacific", {"Belize": "BLZ"}),  # 1, 2, 3, 4, 5, 6
    ("128_132", "non-pacific", {"Cuba": "CUB"}),  # 1, 2, 3, 4, 5, 6, 7
    ("133_128", "non-pacific", {"Haiti": "HTI"}),  # 1, 2, 3, 4, 5, 6
    ("133_130", "non-pacific", {"Cuba": "CUB"}),  # 1, 2, 3, 4, 5, 6, 7
    ("136_132", "non-pacific", {"Turks and Caicos Islands": "TCA"}),  # 1, 2, 4, 6
    ("138_120", "non-pacific", {"Curaçao": "CUW"}),  # 1, 2, 5, 6, 7
    ("138_129", "non-pacific", {"Dominican Republic": "DOM"}),  # 1, 2, 3, 4, 5, 6
    ("141_128", "non-pacific", {"Puerto Rico": "PRI"}),  # 1, 2, 3, 4, 5, 6
    ("143_128", "non-pacific", {"British Virgin Islands": "VGB"}),  # 1, 2, 4, 5, 6
    ("146_121", "non-pacific", {"Saint Vincent and the Grenadines": "VCT"}),  # 1, 2, 5, 6
    ("146_126", "non-pacific", {"Antigua and Barbuda": "ATG"}),  # 1, 2, 3, 4, 5, 6
    ("148_121", "non-pacific", {"Barbados": "BRB"}),  # 1, 2, 3, 5, 6
    ("150_111", "non-pacific", {"Guyana": "GUY"}),  # 1, 2, 3, 4, 6, 7
    ("152_111", "non-pacific", {"Suriname": "SUR"}),  # 1, 2, 3, 4, 5, 6
    ("183_123", "non-pacific", {"Cabo Verde": "CPV"}),  # 1, 2, 6, 7
    ("185_125", "non-pacific", {"Cabo Verde": "CPV"}),  # 2, 5, 6, 7
    ("192_120", "non-pacific", {"Guinea-Bissau": "GNB"}),  # 1, 2, 3, 4, 5, 6, 7
    ("214_104", "non-pacific", {"São Tomé and Príncipe": "STP"}),  # 1, 2, 6
    ("254_091", "non-pacific", {"Seychelles": "SYC"}),  # 1, 2, 4, 6
    ("334_092", "non-pacific", {"Timor-Leste": "TLS"}),  # 1, 2, 3, 4, 5, 6, 7
]
TEST_TILES = PACIFIC_TEST_TILES + NON_PACIFIC_TEST_TILES

# Fixed so the test points are identical every time they are generated.
TEST_DATA_SEED = 0

# (min, max) sample points per class per tile-year. Easy classes (tree cover, water) are capped low so they don't
# dominate, and the hard and medium classes get more points.
HARD_MIN = 500
EASY_MIN = 300
HARD_MAX = 1000
EASY_MAX = 500  # TODO: If rarer classes are confused with tree, then lower this.
CLASS_SAMPLE_N_RANGE = {
    1: (EASY_MIN, EASY_MAX),  # Tree cover
    2: (HARD_MIN, HARD_MAX),  # Grassland
    3: (HARD_MIN, HARD_MAX),  # Cropland
    4: (HARD_MIN, HARD_MAX),  # Wetland
    5: (HARD_MIN, HARD_MAX),  # Built-up
    6: (EASY_MIN, EASY_MAX),  # Water
    7: (HARD_MIN, HARD_MAX),  # Other
}


def _get_pc_client():
    """Return a cached Planetary Computer STAC client."""
    global PC_CLIENT
    if PC_CLIENT is None:
        PC_CLIENT = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1/")
    return PC_CLIENT


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

    crosses_am = isinstance(bbox_across_180(geobox), tuple)

    if crosses_am:
        logger.info("AM-crossing tile, using split load")
        return load_across_antimeridian(lulc_items, geobox, resampling="nearest")

    logger.info("Non-AM-crossing tile, using normal load")
    return stac_load(
        lulc_items,
        geobox=geobox,
        chunks={},
        resampling="nearest",
        patch_url=sign_url,
    ).squeeze(drop=True)


# https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/docs/WorldCover_PUM_V2.0.pdf
# Band 1: Number of Sentinel-1 GAMMA0 observations used in the classification workflow
# Band 2: Number of Sentinel-2 L2A observations used in the classification workflow
def _wc_quality_filter(ds):
    """ESA WorldCover quality filter.

    Keep pixels with at least 1 Sentinel-1 observation and at least 2 Sentinel-2 observations (bands 1 and 2). Values
    below 0 are nodata (-1, and -2 which is undocumented but covers whole files over northern Fiji), so a band only
    fails when its count is known to be too low. Band 3 is not used.
    """
    s1 = ds["input_quality.1"]
    s2 = ds["input_quality.2"]

    # Negative values are nodata, so they pass
    return ((s1 > 0) | (s1 < 0)) & ((s2 > 1) | (s2 < 0))


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
        # First and last years with data on Planetary Computer.
        "years": (2020, 2021),
        "native_band": "map",
        "output_band": "esa_wc",
        "class_map": world_cover_map,
        "quality_fn": _wc_quality_filter,
        "quality_bands": ["input_quality.1", "input_quality.2"],
    },
    {
        "product": "esa-cci-lc",
        "years": (1992, 2020),
        "native_band": "lccs_class",
        "output_band": "esa_cci",
        "class_map": cci_lc_map,
        "quality_fn": _cci_quality_filter,
        "quality_bands": ["processed_flag", "change_count", "observation_count"],
    },
    {
        "product": "io-lulc-annual-v02",
        "years": (2017, 2023),
        "native_band": "data",
        "output_band": "io",
        "class_map": io_map,
        "quality_fn": None,
        "quality_bands": [],
    },
]


def lulc_products_for_year(year: str) -> list[dict]:
    """The LULC_PRODUCTS with data for a year. At least 2 are needed to find agreement.

    Args:
        year: Year string (e.g. "2020").

    Returns:
        Product configuration dicts from LULC_PRODUCTS.

    Raises:
        LdnError: If fewer than 2 products have data for the year.
    """
    products = [product for product in LULC_PRODUCTS if product["years"][0] <= int(year) <= product["years"][1]]
    if len(products) < 2:
        coverage = ", ".join(f"{p['product']} {p['years'][0]}-{p['years'][1]}" for p in LULC_PRODUCTS)
        raise LdnError(f"{len(products)} LULC product(s) have data for {year}, at least 2 are needed ({coverage})")
    return products


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
    native_band, output_band, quality_fn = (
        product_dict["native_band"],
        product_dict["output_band"],
        product_dict["quality_fn"],
    )
    quality_bands = product_dict["quality_bands"]

    ds = load_lulc_for_tile(product_dict["product"], geobox, year)
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
    ds[output_band] = standardise_class(ds[output_band], product_dict["class_map"])

    country_prj = country_wgs84_buffered.to_crs(geobox.crs)
    clip_geom = Geometry(country_prj.union_all(), crs=geobox.crs)
    ds = ds.odc.crop(clip_geom, apply_mask=True, all_touched=True)

    for var in ds.data_vars:
        ds[var] = ds[var].fillna(LULC_NODATA).astype("uint8")

    ds[output_band] = ds[output_band].where(ds[output_band] != 0, LULC_NODATA)
    ds[output_band].attrs["nodata"] = LULC_NODATA

    return ds


def find_agreement(products: list[xr.DataArray]) -> xr.DataArray:
    """Find where at least 2 products agree on a class, with neighbourhood filtering.

    With 3 products at most one class can have 2 votes, so the first agreeing pair gives the class.

    Args:
        products: 2 or 3 standardised class DataArrays on the same grid, with 0 or LULC_NODATA for nodata.

    Returns:
        DataArray of agreed class values, LULC_NODATA where fewer than 2 products agree or a pixel has a
        neighbour without agreement.
    """
    # Drop leftover scalar coordinates that cause merge conflicts.
    products = [da.drop_vars([c for c in da.coords if c not in da.dims]) for da in products]
    agreed = xr.zeros_like(products[0])
    for first, second in combinations(products, 2):
        pair = (first == second) & (first > 0) & (first != LULC_NODATA)
        agreed = agreed.where(agreed > 0, first.where(pair, 0))

    # Keep a pixel only if its whole 3x3 neighbourhood agrees, dropping isolated agreeing pixels.
    neighbourhood = minimum_filter((agreed > 0).values.astype("uint8"), size=3, mode="constant", cval=0) == 1
    agreed = agreed.where(neighbourhood & (agreed > 0), LULC_NODATA).rename(CLASS_ATTR).astype("uint8")
    agreed.attrs["nodata"] = LULC_NODATA
    return agreed


def generate_samples(
    agreed: xr.DataArray,
    geomad_dem_indices: xr.Dataset,
    n: int,
    seed: int | None,
) -> gpd.GeoDataFrame:
    """Generate stratified random samples from the agreement map.

    Args:
        agreed: Agreement classification DataArray.
        geomad_dem_indices: GeoMAD dataset (used to mask nodata areas).
        n: Number of sample points shared between classes by area, before CLASS_SAMPLE_N_RANGE clips each class.
        seed: Random seed for reproducible sampling. None for non-reproducible sampling.

    Returns:
        GeoDataFrame of sample points with class labels.
    """
    # The agreement map is loaded on the features' geobox and cropped to the same country, so they align exactly.
    # join="exact" raises if they ever don't.
    agreed, geomad_valid = xr.align(agreed, geomad_dem_indices["red"].notnull(), join="exact")
    agree_masked = agreed.where(geomad_valid, other=LULC_NODATA).compute()

    # Trim to bounding box of valid pixels
    valid_mask = agree_masked.values != LULC_NODATA
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

    samples = random_sampling(agree_for_sampling, n, CLASS_SAMPLE_N_RANGE, seed)
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


def _upload_dataframe_csv_to_s3(df, bucket: str, key: str):
    """Upload a dataframe as CSV to S3 and return the S3 URI.

    Args:
        df: DataFrame to upload.
        bucket: S3 bucket name.
        key: Key path within the bucket.

    Returns:
        S3 URI of the uploaded file.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    logger.info(f"Uploaded CSV to {get_public_url_base(bucket)}/{key}")


# Dep tools utils have mask_to_gadm() which would be helpful, but I want to buffer gadm before masking.
def get_buffered_country(
    country_of_interest: dict[str, str],
    analysis_crs: Literal["EPSG:3832", "EPSG:6933"],
    buffer_m: int = 100,
) -> gpd.GeoDataFrame:
    """Fetch and buffer a country geometry for analysis (antimeridian-fixed).

    Retrieves country geometry from GADM, applies country-specific clipping for
    known edge cases (for example antimeridian handling for Fiji), buffers in
    the analysis CRS, and returns the result in WGS84.

    Args:
        country_of_interest: Mapping of country name to country code (single-item
            dictionary expected).
        analysis_crs: Projected CRS string used for buffering in meters.
        buffer_m: Optional; buffer distance in meters applied to the country geometry.

    Returns:
        A GeoDataFrame containing buffered country geometry in `WGS84`.
    """
    country_gadm = get_gadm(countries=country_of_interest)

    country_gadm = gpd.GeoDataFrame(
        geometry=country_gadm.to_crs(analysis_crs).buffer(buffer_m).to_crs(WGS84),
        crs=WGS84,
    )
    # Do antimeridian fix. Needed for Fiji.
    rows = []
    for geom in country_gadm.geometry:
        # Buffering gives clockwise exteriors. fix_winding=True reverses them as before, without warning.
        fix = fix_multi_polygon if geom.geom_type == "MultiPolygon" else fix_polygon
        fixed = fix(geom, fix_winding=True)
        # The antimeridian fix can return self-intersecting polygons (e.g. Belize), which break later intersections.
        fixed = shapely.make_valid(fixed, method="structure", keep_collapsed=False)
        if fixed.geom_type == "MultiPolygon":
            rows.extend(fixed.geoms)  # one row per polygon (east/west of AM)
        else:
            rows.append(fixed)

    return gpd.GeoDataFrame(geometry=rows, crs=WGS84)


def make_training_data(
    tile_id: str,
    year: str,
    region: Literal["pacific", "non-pacific"],
    geomad_version: str,
    geomad_bucket: str,
    output_bucket: str,
    country_of_interest: dict[str, str],
    product_owner_geomad: str | None,
    single_region_geomad: bool,
    file_prefix: str,
    output_key: str,
    n: int,
    sensor: str,
    seed: int | None,
):
    """Generate training data for a single tile and upload to S3 as CSV.

    End-to-end pipeline: loads GeoMAD (from any bucket), loads the 2 or 3 LULC products with data for the year,
    finds agreement, samples, extracts band values, filters outliers, and
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
        n: Number of sample points shared between classes by area, before CLASS_SAMPLE_N_RANGE clips each class.
        product_owner_geomad: Optional owner override for the GeoMAD data (e.g. "dep" or "ci").
        single_region_geomad: If True, search the owner's single region GeoMAD index, otherwise the many-region index.
        file_prefix: Local path prefix for the output files, relative to ldn/.
        output_key: S3 key for the output CSV.
        seed: Random seed for reproducible sampling. None for non-reproducible sampling.

    Returns:
        GeoDataFrame of final training samples.
    """
    # Fail before loading anything if the year doesn't have enough products to agree.
    products = lulc_products_for_year(year)

    logger.info("Getting buffered country geometry")
    analysis_crs = get_analysis_epsg(region)
    country_wgs84_buffered = get_buffered_country(country_of_interest, analysis_crs)

    # Clip the country geometry to this tile's footprint: it fails early if the country isn't in this tile, and keeps
    # the polygons used for the final feature clip and LULC product masks small (e.g. Kiribati spans a huge area).
    # Skip for AM-crossing tiles: their WGS84 footprint straddles ±180° and intersects incorrectly with standard WGS84
    # country geometries.
    logger.info("Clipping buffered country geometry to tile footprint")
    tile_id_tuple = parse_tile_id(tile_id)
    grid = get_gridspec(region=region)
    tile_geobox = grid.tile_geobox(tile_id_tuple)
    tile_crosses_am = isinstance(bbox_across_180(tile_geobox), tuple)
    if tile_crosses_am:
        logger.info("AM-crossing tile — skipping country clip to tile footprint")
    else:
        country_wgs84_buffered = gpd.GeoDataFrame(
            geometry=country_wgs84_buffered.intersection(tile_geobox.footprint(WGS84).geom),
            crs=WGS84,
        )
        # Drop parts of the country outside the tile. Intersection gives empty (never missing) geometries.
        country_wgs84_buffered = country_wgs84_buffered[~country_wgs84_buffered.is_empty]
        if country_wgs84_buffered.empty:
            raise LdnError(f"Country geometry does not overlap tile {tile_id}")
        logger.info("Clipped country geometry to tile footprint")

    logger.info("Loading GeoMAD, DEM, and indices")
    geomad_dem_indices = search_and_load_geomad_indices_dem(
        tile_id,
        year,
        region=region,
        country_wgs84_buffered=country_wgs84_buffered,
        product_owner=product_owner_geomad,
        single_region=single_region_geomad,
        bucket=geomad_bucket,
        geomad_version=geomad_version,
        sensor=sensor,
    )
    geobox = geomad_dem_indices.odc.geobox

    logger.info(f"Loading {len(products)} LULC products: {', '.join(p['product'] for p in products)}")
    agreed = find_agreement(
        [load_and_prepare(geobox, country_wgs84_buffered, p, year)[p["output_band"]] for p in products]
    )

    logger.info("Generating samples")
    samples = generate_samples(agreed, geomad_dem_indices, n=n, seed=seed)

    logger.info("Extracting GeoMAD values at sample points")
    samples = extract_geomad_dem_indices_values(samples, geomad_dem_indices, analysis_crs)

    n_samples = len(samples)
    samples = remove_nan_samples(samples)
    logger.info(f"Removed {n_samples - len(samples)} of {n_samples} samples with NaN features")

    # TODO: Run a baseline version and then test if outlier filtering improves the model and classification.
    # logger.info("Filtering outliers")
    # samples = filter_outliers(samples)

    out_fname_local = f"ldn/{file_prefix}"
    Path(out_fname_local).parent.mkdir(parents=True, exist_ok=True)
    samples.to_file(f"{out_fname_local}.geojson", driver="GeoJSON", index=False)
    samples.to_csv(f"{out_fname_local}.csv", index=False)
    logger.info(f"Saved training data to {out_fname_local}")

    _upload_dataframe_csv_to_s3(samples, output_bucket, output_key)


@cli_training_app.command()
def generate_training_data(
    tile_id: str = typer.Option(..., help="Grid tile identifier (e.g. 058_043)"),
    year: str = typer.Option(..., help="Year (e.g. 2020)"),
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
    n: int = typer.Option(
        2100, help="Sample points shared between classes by area, before CLASS_SAMPLE_N_RANGE clips each class"
    ),
    overwrite: bool = typer.Option(False, help="Whether to overwrite existing data in S3"),
    product_owner_geomad: str | None = typer.Option(
        None, help="Override the region's default owner prefix (e.g. 'dep') for the GeoMAD data"
    ),
    product_owner_output: str | None = typer.Option(
        None, help="Override the region's default owner prefix (e.g. 'dep') for the output data"
    ),
    single_region_geomad: bool = typer.Option(
        ...,
        help="Whether to use the single region prefix (e.g. 'dep_ls_geomad') "
        "or the generic prefix (e.g. 'ls_geomad') when accessing GeoMAD data.",
    ),
    single_region_output: bool = typer.Option(
        ...,
        help="Whether to use the single region prefix (e.g. 'dep_ls_lulc') "
        "or the generic prefix (e.g. 'ls_lulc') when writing output training data.",
    ),
    sensor: str = typer.Option(help="Sensor name (e.g. 'ls')", default=SENSOR),
    split: Literal["train", "test"] = typer.Option(
        "train", help="'test' makes the frozen held-out test set (TEST_TILES) instead of training data"
    ),
):
    """Generate training or test data for LULC classification.
    Read geomad from any bucket and write the data to any bucket.
    """
    output_bucket = output_bucket or get_env_var("BUCKET")  # Default
    if not output_bucket or not geomad_bucket:
        raise LdnError("Output bucket and GeoMAD bucket must be set.")
    country_of_interest = {country_name: country_code}

    logger.info(
        f"Creating training data for tile {tile_id}, year {year}, region {region}, geomad_bucket "
        f"{geomad_bucket}, output_bucket {output_bucket}, country {country_of_interest}, n={n}, overwrite={overwrite}, "
        f"product_owner_geomad={product_owner_geomad}, product_owner_output={product_owner_output}, "
        f"training_data_version={training_data_version}, geomad_version={geomad_version}, sensor={sensor}"
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

    is_test_tile = (tile_id_x, tile_id_y) in {parse_tile_id(t[0]) for t in TEST_TILES}
    if split == "train" and is_test_tile:
        raise LdnError(f"Tile {tile_id} is a test tile and must not be used for training data.")
    if split == "test" and not is_test_tile:
        raise LdnError(f"Tile {tile_id} is not in TEST_TILES.")
    if split == "test" and overwrite:
        raise LdnError("Test data is frozen and cannot be overwritten. Bump TEST_DATA_VERSION to regenerate it.")
    data_dir, data_version, seed = (
        ("test_data", TEST_DATA_VERSION, TEST_DATA_SEED)
        if split == "test"
        else ("training_data", training_data_version, None)
    )

    # Zero padded indexes
    file_prefix = f"{data_dir}/{data_version}/{region}/{tile_id_x:03d}/{tile_id_y:03d}/{year}/samples"
    # Training data shouldn't be written to source.coop, but supported just in case.
    _is_bucket_source_coop = is_bucket_source_coop(output_bucket)
    if _is_bucket_source_coop:
        raise NotImplementedError("Writing training data to Source.Coop is not supported.")
        # file_prefix = f"{SOURCE_COOP_PREFIX_LULC}/{file_prefix}"
    output_owner = owner_for_region(region, product_owner_output) if single_region_output else None
    output_key = f"{dataset_prefix(output_owner, sensor, LULC_DATASET_ID)}/{file_prefix}.csv"

    if not overwrite:
        logger.info(f"Overwrite is False, checking if object exists at s3://{output_bucket}/{output_key}")
        exists = object_exists(output_bucket, output_key)
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
        product_owner_geomad,
        single_region_geomad,
        file_prefix,
        output_key,
        n,
        sensor,
        seed,
    )


def search_and_load_geomad_indices_dem(
    tile_id: str,
    year: str,
    region: Literal["pacific", "non-pacific"],
    country_wgs84_buffered: gpd.GeoDataFrame,
    product_owner: str | None,
    single_region: bool,
    bucket: str,
    geomad_version: str,
    sensor: str,
) -> xr.Dataset:
    """Search, load, scale, and merge GeoMAD bands, spectral indices, and DEM terrain for a tile.
        Supports antimeridian-crossing tiles. The result is clipped to the buffered country.

    Args:
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string for the GeoMAD item search (e.g. "2020").
        region: Grid region, either "pacific" or "non-pacific".
        country_wgs84_buffered: Buffered country geometry in WGS84, used for the final clip.
        product_owner: Optional owner override (e.g. "dep" or "ci").
        single_region: If True, search the owner's single region GeoMAD index, otherwise the many-region index.
        bucket: S3 bucket name where the GeoMAD data is stored.
        geomad_version: GeoMAD version string (e.g. "0-3-0").

    Returns:
        Merged dataset with GeoMAD bands, spectral indices, elevation,
        slope, northness and eastness, clipped to the intersection of the tile and the buffered country.
    """
    logger.info(f"Searching for GeoMAD item for tile {tile_id} and year {year}.")
    if GEOMAD_VERSION != geomad_version:
        logger.info(f"Using overridden GeoMAD version {geomad_version} instead of default {GEOMAD_VERSION}")
    else:
        logger.info(f"Using latest GeoMAD version {GEOMAD_VERSION}")

    # The tile's item (raises if missing) and any neighbouring items to fill the focal statistics padding.
    # Gap fill years is disabled (set to 0) so only this year's data is considered for training data.
    geomad_items = GeomadSearcher(
        bucket, region, product_owner, single_region, sensor, geomad_version, year, parse_tile_id(tile_id), 0
    ).search()
    logger.info(f"Found GeoMAD item {geomad_items[0].id} and {len(geomad_items) - 1} neighbours for year {year}")

    bands = [b for b in geomad_items[0].assets.keys() if b != "count"]
    logger.info(f"Loading bands: {bands}")

    # The tile geobox is the GeoMAD items' native grid, so this loads with no reprojection. Padded so focal
    # statistics are complete at tile edges, the same as prediction.
    tile_geobox = get_gridspec(region=region).tile_geobox(parse_tile_id(tile_id))
    geomad_ds = stac_load(
        geomad_items,
        # Force lazy, one chunk per band. 1024 px chunks were about 2x slower for a padded tile (99 s vs ~200 s).
        chunks={},
        bands=bands,
        fail_on_error=True,  # We control the data so it shouldn't fail.
        geobox=tile_geobox.pad(FOCAL_PAD),
    ).squeeze()
    logger.info(f"GeoMAD shape (padded): {dict(geomad_ds.sizes)}")

    # All candidate focal statistics, so model training can choose which to keep.
    merged = crop_to_geobox(make_features(geomad_ds, FOCAL_FEATURES), tile_geobox)
    logger.info(f"Merged GeoMAD+DEM shape: {dict(merged.sizes)}")

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

    logger.info(f"Merged GeoMAD/DEM shape (after country clip): {dict(merged.sizes)}")
    return merged
