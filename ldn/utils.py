import logging
from typing import Literal

import numpy as np
import xarray as xr
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES
from dep_tools.utils import bbox_across_180, search_across_180
from geopandas import GeoDataFrame
from odc.geo.geobox import GeoBox
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import ItemCollection
from pystac_client import Client as PyStacClient
from rasterio.enums import Resampling
from scipy.ndimage import sobel
from shapely.geometry import box

logger = logging.getLogger(__name__)

SIDS_COUNTRIES_AND_CODES = {
    # Caribbean
    "Anguilla": "AIA",
    "Antigua and Barbuda": "ATG",
    "Aruba": "ABW",
    "Bahamas": "BHS",
    "Barbados": "BRB",
    "Belize": "BLZ",
    "Bermuda": "BMU",
    "British Virgin Islands": "VGB",
    "Cayman Islands": "CYM",
    "Cuba": "CUB",
    "Curaçao": "CUW",
    "Dominica": "DMA",
    "Dominican Republic": "DOM",
    "Grenada": "GRD",
    "Guadeloupe": "GLP",
    "Guyana": "GUY",
    "Haiti": "HTI",
    "Jamaica": "JAM",
    "Martinique": "MTQ",
    "Montserrat": "MSR",
    "Puerto Rico": "PRI",
    "Saint Kitts and Nevis": "KNA",
    "Saint Lucia": "LCA",
    "Saint Vincent and the Grenadines": "VCT",
    "Sint Maarten": "SXM",
    "Suriname": "SUR",
    "Trinidad and Tobago": "TTO",
    "Turks and Caicos Islands": "TCA",
    "Virgin Islands, U.S.": "VIR",
    # Pacific
    "American Samoa": "ASM",
    "Cook Islands": "COK",
    "Fiji": "FJI",
    "French Polynesia": "PYF",
    "Guam": "GUM",
    "Kiribati": "KIR",
    "Marshall Islands": "MHL",
    "Micronesia": "FSM",
    "Nauru": "NRU",
    "New Caledonia": "NCL",
    "Niue": "NIU",
    "Northern Mariana Islands": "MNP",
    "Palau": "PLW",
    "Papua New Guinea": "PNG",
    "Samoa": "WSM",
    "Solomon Islands": "SLB",
    "Timor-Leste": "TLS",
    "Tonga": "TON",
    "Tuvalu": "TUV",
    "Vanuatu": "VUT",
    # Africa, Indian Ocean, Mediterranean, South China Sea (AIMS)
    "Cabo Verde": "CPV",
    "Comoros": "COM",
    "Guinea-Bissau": "GNB",
    "Maldives": "MDV",
    "Mauritius": "MUS",
    "São Tomé and Príncipe": "STP",
    "Seychelles": "SYC",
    "Singapore": "SGP",
}

# Merge dicts, DEP override SIDS if duplicate name
ALL_COUNTRIES = {**SIDS_COUNTRIES_AND_CODES, **DEP_COUNTRIES_AND_CODES}

# Get SIDS countries that are not in DEP for CI Grid use.
NON_DEP_COUNTRIES = {k: v for k, v in SIDS_COUNTRIES_AND_CODES.items() if k not in DEP_COUNTRIES_AND_CODES}

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


GEOMAD_VERSION = "0-2-1"
PREDICTION_VERSION = "0-0-4"
MODEL_VERSION = "0-0-4"
TRAINING_DATA_VERSION = "0-0-4"

# aws s3 cp test.txt s3://us-west-2.opendata.source.coop/auspatious/geomad-sids/test.txt
# aws s3 rm s3://us-west-2.opendata.source.coop/auspatious/geomad-sids/test.txt
# aws s3 rm s3://us-west-2.opendata.source.coop/auspatious/lulc-sids/dep_ls_lulc_prediction/0-0-4-test/ --recursive
SOURCE_COOP_PUBLIC_URL = "https://data.source.coop"  # public read URL for STAC hrefs
SOURCE_COOP_PREFIX_GEOMAD = "auspatious/geomad-sids"  # For source.coop.
SOURCE_COOP_PREFIX_PREDICTION = "auspatious/lulc-sids"  # For source.coop.
# SOURCE_COOP_PUBLIC_URL = None # For non-Source.Coop buckets.
# SOURCE_COOP_PREFIX_GEOMAD = None
# SOURCE_COOP_PREFIX_PREDICTION = None

# BUCKET = "data.ldn.auspatious.com"
# BUCKET = "dep-public-staging"
BUCKET = "us-west-2.opendata.source.coop"


PACIFIC_OWNER = "dep"
NON_PACIFIC_OWNER = "ci"

SENSOR = "ls"
GEOMAD_DATASET_ID = "geomad"
PREDICTION_DATASET_ID = "lulc_prediction"
AWS_REGION = "us-west-2"

LS7_YEAR_THRESHOLD = 2012
training_data_year = "2020"

class_attr = "lulc"

wgs84 = "EPSG:4326"


def owner_for_region(
    region: Literal["pacific", "non-pacific"],
    owner_pacific: str = PACIFIC_OWNER,
    owner_non_pacific: str = NON_PACIFIC_OWNER,
    product_owner: str | None = None,
) -> str:
    """Return the short owner prefix for a given region (e.g. 'dep' or 'ci').

    If product_owner is set, it overrides the region-based lookup.
    """
    if product_owner is not None:
        return product_owner
    return owner_pacific if region == "pacific" else owner_non_pacific


def dataset_prefix(owner: str, dataset_id: str) -> str:
    """Build the full dataset prefix from owner and dataset_id.

    Args:
        owner: Short owner prefix (e.g. "dep" or "ci").
        dataset_id: Dataset identifier (e.g. "geomad" or "lulc_prediction").

    Returns:
        Full prefix like "dep_ls_geomad" or "ci_ls_lulc_prediction".
    """
    return f"{owner}_{SENSOR}_{dataset_id}"


def get_geomad_stac_geoparquet_url(
    region: Literal["pacific", "non-pacific"],
    bucket: str,
    product_owner: str | None = None,
    version: str | None = None,
) -> str:
    """Build the STAC-Geoparquet URL for GeoMAD data in a given region.

    Args:
        region: Either "pacific" or "non-pacific".
        product_owner: Optional override for the region-derived owner prefix.
        version: GeoMAD version string. Defaults to GEOMAD_VERSION.

    Returns:
        HTTPS URL to the STAC-Geoparquet file.
    """
    ver = version if version is not None else GEOMAD_VERSION
    owner = owner_for_region(region, product_owner=product_owner)
    prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
    if SOURCE_COOP_PUBLIC_URL:
        return f"{SOURCE_COOP_PUBLIC_URL}/{SOURCE_COOP_PREFIX_GEOMAD}/{prefix}/{ver}/{prefix}.parquet"
    return f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}/{prefix}/{ver}/{prefix}.parquet"


def get_analysis_epsg(
    region: Literal["pacific", "non-pacific"],
) -> Literal["EPSG:3832", "EPSG:6933"]:
    if region == "pacific":
        return "EPSG:3832"
    else:
        return "EPSG:6933"


# Our custom exception class for the project. Good for filtering errors in processing.
class LdnError(Exception):
    """Base exception for the ldn-lulc project."""


def scale_offset_landsat(data: xr.Dataset) -> xr.Dataset:
    """Scale Landsat Collection 2 reflectance values and mask nodata.

    Applies the USGS scaling formula: scaled = raw * 0.0000275 - 0.2,
    clips to [0, 1], and replaces nodata pixels with NaN.

    Modifies the dataset in place and returns it.

    Args:
        data: Input dataset with Landsat integer reflectance bands.
            Expected nodata values are 0 and 65535.

    Returns:
        The same dataset with bands scaled to float32 in [0, 1].
    """
    # Landsat Collection 2 scaling constants (USGS)
    scale_factor = 0.0000275
    offset = -0.2
    nodata_values = (0, 65_535)

    bands_to_scale = [band for band in data.data_vars if band not in ["count", "emad", "smad", "bcmad"]]

    for band in bands_to_scale:
        raw = data[band]
        nodata = (raw == nodata_values[0]) | (raw == nodata_values[1])
        # TODO: Clip to 0.01 instead of 0 so indices still work? Also would clipping to 0 mean that this becomes nodata?
        scaled = (raw * scale_factor + offset).clip(0, 1).astype("float32")
        data[band] = scaled.where(~nodata, other=np.nan)
    return data


def calculate_indices(geomad: xr.Dataset) -> xr.Dataset:
    """Compute spectral indices from scaled geomedian bands.

    Adds index bands to the dataset in place. Division-by-zero cases
    (e.g. when both bands are 0 or NaN) will produce NaN values.

    Args:
        geomad: GeoMedian/GeoMAD dataset containing at least `nir08`, `red`, `green`,
            `blue`, `swir16`, and `swir22` bands (scaled to [0, 1]).

    Returns:
        The same dataset with additional bands: `ndvi`, `ndwi`, `mndwi`,
        `ndti`, `bsi`, `mbi`, `baei`, and `bui`.
    """
    nir = geomad.nir08
    red = geomad.red
    green = geomad.green
    blue = geomad.blue
    swir1 = geomad.swir16
    swir2 = geomad.swir22
    geomad["ndvi"] = (nir - red) / (nir + red)
    geomad["ndwi"] = (green - nir) / (green + nir)
    geomad["mndwi"] = (green - swir1) / (green + swir1)
    geomad["ndti"] = (red - green) / (red + green)
    geomad["bsi"] = ((swir1 + red) - (nir + blue)) / ((swir1 + red) + (nir + blue))
    geomad["mbi"] = ((swir1 - swir2 - nir) / (swir1 + swir2 + nir)) + 0.5
    geomad["baei"] = (red + 0.3) / (green + swir1)
    ndbi = (swir1 - nir) / (swir1 + nir)  # intermediate, not stored
    geomad["bui"] = ndbi - geomad["ndvi"]
    return geomad


def _compute_terrain(dem_da: xr.DataArray) -> xr.Dataset:
    """Compute slope and aspect from an elevation DataArray.

    Uses Sobel filters to estimate terrain gradients. The pixel
    resolution is assumed to be in meters (projected CRS).

    Args:
        dem_da: 2D elevation DataArray with x/y coordinates.

    Returns:
        Dataset with elevation, slope (degrees), and aspect (degrees).
    """
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem_da.x[1] - dem_da.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope = xr.DataArray(
        np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))),
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="slope",
    )
    aspect = xr.DataArray(
        (90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="aspect",
    )

    return xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})


# GeoMAD output bands that the prediction pipeline needs (excludes "count").
GEOMAD_BANDS = [
    "nir08",
    "red",
    "green",
    "blue",
    "swir16",
    "swir22",
    "smad",
    "bcmad",
    "emad",
]

# Copernicus DEM collection on MS PC.
DEM_CATALOG = "https://planetarycomputer.microsoft.com/api/stac/v1/"
DEM_COLLECTION = "cop-dem-glo-30"


def _load_dem_am(
    dem_items: ItemCollection,
    geobox: GeoBox,
    geobox_wgs84: GeoDataFrame,
) -> xr.Dataset:
    """Load DEM for a tile that crosses the antimeridian.

    This is needed to prevent a memory error due to loading a world-spanning DEM tile when the WGS84
    footprint overlaps both sides of the AM.
    Can't load straight to target geobox CRS. Must load to WGS84, shift longitudes,
    concatenate, then reproject with +over.

    Loads east and west halves separately in WGS84, shifts west
    longitudes to >180, concatenates, and reprojects to the target
    geobox using the PROJ "+over" flag.

    The "+over" CRS flag tells PROJ to allow longitudes >180 instead
    of wrapping them. This is required because:
    - stac_load(geobox=) cannot reproject WGS84 data across the AM
      into EPSG:3832 (PROJ maps -180 to the wrong side of the projection).
    - odc.reproject does not support +over CRS (returns all NaN).
    - rioxarray's rio.reproject wraps rasterio.warp.reproject, which
      handles +over correctly.

    Related open issues (no upstream fix as of 2025-04):
    - https://github.com/opendatacube/odc-stac/issues/165
    - https://github.com/opendatacube/odc-stac/issues/172
    - https://github.com/opendatacube/odc-geo/issues/208

    Args:
        dem_items: STAC items from search_across_180.
        geobox: Target geobox in the analysis CRS (e.g. EPSG:3832).
        geobox_wgs84: Tile footprint as a WGS84 GeoDataFrame.

    Returns:
        Dataset with a single "elevation" variable in the target CRS.
    """
    east_bbox, west_bbox = bbox_across_180(geobox_wgs84)
    east_gdf = GeoDataFrame(geometry=[box(*east_bbox)], crs=wgs84)
    west_gdf = GeoDataFrame(geometry=[box(*west_bbox)], crs=wgs84)

    east_items = [i for i in dem_items if i.bbox[0] >= 0]
    west_items = [i for i in dem_items if i.bbox[0] < 0]

    halves = []
    for items, gdf in [(east_items, east_gdf), (west_items, west_gdf)]:
        if not items:
            continue
        ds = (
            stac_load(
                items,
                geopolygon=gdf,
                chunks={},  # Force lazy.
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            ).squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
        )
        halves.append(ds)

    if len(halves) != 2:
        raise LdnError(
            f"Expected to load 2 halves of the DEM but got {len(halves)}. Check if the tile geometry is "
            f"correct and if the DEM items cover the area."
        )

    # Shift west longitudes (-180..-179) to (180..181) so the
    # two halves form a continuous longitude range.
    halves[1] = halves[1].assign_coords(longitude=(halves[1].longitude % 360))
    ds_combined = xr.concat(halves, dim="longitude").sortby("longitude")

    # rio is required here because odc.reproject does not support +over.
    ds_combined = ds_combined.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    ds_combined = ds_combined.rio.write_crs("+proj=longlat +datum=WGS84 +over")
    return ds_combined.rio.reproject(
        str(geobox.crs),
        shape=(geobox.height, geobox.width),
        transform=geobox.transform,
        resampling=Resampling.bilinear,
    ).rename({"data": "elevation"})


def load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, and aspect.

    Loads COP-DEM-GLO-30 tiles from Planetary Computer, reprojects
    to the target geobox, and derives terrain features using Sobel
    filters. Handles antimeridian-crossing tiles via _load_dem_am.

    Args:
        geobox: Target grid (of a tile) in the analysis CRS (EPSG:3832 or EPSG:6933).

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    # AM-crossing-safe search.
    dem_items = search_across_180(geobox, client, collections=[DEM_COLLECTION])
    logger.info(f"Found {len(dem_items)} DEM items")

    if len(dem_items) == 0:
        raise LdnError("No DEM items found. COP-DEM-GLO-30 is global so this is unexpected.")
    if len(dem_items) >= 10:
        raise LdnError(f"Too many DEM items ({len(dem_items)}). Expected ~4, data may be world-spanning.")

    geobox_wgs84 = GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(wgs84)
    crosses_am = isinstance(bbox_across_180(geobox_wgs84), tuple)

    if crosses_am:
        logger.info("Tile crosses the antimeridian, using custom '+over' loading logic")
        dem = _load_dem_am(dem_items, geobox, geobox_wgs84)
    else:
        logger.info("Tile does not cross the antimeridian, using standard loading logic")
        dem = (
            stac_load(
                dem_items,
                geobox=geobox,
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
                chunks={},  # Force lazy
            )
            .squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
            .rename({"data": "elevation"})
        )

    # Assign CRS so spatial_ref matches GeoMAD during xr.merge.
    dem = dem.odc.assign_crs(crs=geobox.crs)

    logger.info(f"DEM elevation shape: {dem['elevation'].shape}")

    return _compute_terrain(dem["elevation"])


def parse_years(years: str) -> list[int]:
    """Parse a years string into a list of integers.

    Accepts a comma-separated list (e.g. '2020,2021') or a range (e.g. '2010-2023').
    """
    if "," in years:
        return [int(y.strip()) for y in years.split(",")]
    elif "-" in years:
        start_year, end_year = map(int, years.split("-"))
        return list(range(start_year, end_year + 1))
    else:
        return [int(years)]
