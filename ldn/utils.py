import logging
import re
from typing import Literal

from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

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

# TODO: Should these be env vars instead e.g. SOURCE_COOP_PUBLIC_URL?

# Source.Coop setup:
# SOURCE_COOP_PUBLIC_URL = "https://data.source.coop"  # For source.coop.
# SOURCE_COOP_PREFIX_GEOMAD = "auspatious/geomad-sids"  # For source.coop.
# SOURCE_COOP_PREFIX_PREDICTION = "auspatious/lulc-sids"  # For source.coop.
# BUCKET = "us-west-2.opendata.source.coop"  # For source.coop.

# Non-Source.Coop setup:
SOURCE_COOP_PUBLIC_URL = None  # For non-source.coop.
SOURCE_COOP_PREFIX_GEOMAD = None  # For non-source.coop.
SOURCE_COOP_PREFIX_PREDICTION = None  # For non-source.coop.
BUCKET = "data.ldn.auspatious.com"  # Auspatious custom domain bucket
# BUCKET = "dep-public-staging" # DEP Staging (typical bucket)
# BUCKET = "dep-public-data" # DEP Prod (typical bucket)

PACIFIC_OWNER = "dep"
NON_PACIFIC_OWNER = "ci"

SENSOR = "ls"
GEOMAD_DATASET_ID = "geomad"
PREDICTION_DATASET_ID = "lulc_prediction"
AWS_REGION = "us-west-2"

LS7_YEAR_THRESHOLD: int = 2012
TRAINING_DATA_YEAR: str = "2020"
CLASS_ATTR: str = "lulc"
WGS84: str = "EPSG:4326"


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


def dataset_prefix(owner: str | None, dataset_id: str) -> str:
    """Build the full dataset prefix from owner, sensor, and dataset_id (owner optional).

    Args:
        owner: Short owner prefix (e.g. "dep" or "ci").
        dataset_id: Dataset identifier (e.g. "geomad" or "lulc_prediction").

    Returns:
        Full prefix like "dep_ls_geomad" or "ci_ls_lulc_prediction" or "ls_geomad" if owner is None.
    """
    if owner:
        return f"{owner}_{SENSOR}_{dataset_id}"
    else:
        return f"{SENSOR}_{dataset_id}"


def get_stac_geoparquet_key(
    dataset_id: str,
    version: str,
    source_coop_prefix: str | None = None,
) -> str:
    """Return the S3 key for the STAC-Geoparquet file (no bucket, no scheme).

    Args:
        dataset_id: The dataset ID (e.g. 'geomad').
        version: Version string.
        source_coop_prefix: Source.Coop prefix if applicable, else None.

    Returns:
        S3 key string e.g. 'auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet'
        or 'ls_geomad/0-2-1/ls_geomad.parquet' for a standard bucket.
    """
    combined_short = dataset_prefix(None, dataset_id)
    key = f"{combined_short}/{version}/{combined_short}.parquet"
    if source_coop_prefix:
        return f"{source_coop_prefix}/{key}"
    return key


def get_geomad_stac_geoparquet_url(bucket: str, version: str) -> str:
    """Return the URL to the GeoMAD STAC-Geoparquet file for use with rustac/DuckDB.

    For Source.Coop, returns a public HTTPS URL (publicly readable).
    For all other buckets, returns a regional S3 HTTPS URL since DuckDB needs
    a resolvable endpoint and cannot handle custom-domain buckets directly.

    Args:
        bucket: The S3 bucket name or custom domain.
        version: GeoMAD version string.

    Returns:
        URL to the STAC-Geoparquet file.
    """
    key = get_stac_geoparquet_key(GEOMAD_DATASET_ID, version, SOURCE_COOP_PREFIX_GEOMAD)
    if SOURCE_COOP_PUBLIC_URL:
        return f"{SOURCE_COOP_PUBLIC_URL}/{key}"
    return f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}/{key}"


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


# TODO: _source_coop_prefix() returns source_coop_prefix
def resolve_dataset(
    dataset: Literal["geomad", "prediction"],
    version_geomad: str,
    version_prediction: str,
) -> tuple[str, str, str | None]:
    """Return (dataset_id, version, source_coop_prefix) for the given dataset name."""
    if dataset == "geomad":
        return GEOMAD_DATASET_ID, version_geomad, SOURCE_COOP_PREFIX_GEOMAD
    return PREDICTION_DATASET_ID, version_prediction, SOURCE_COOP_PREFIX_PREDICTION


def parse_tile_id(tile_id: str) -> tuple[int, int]:
    """Parse a tile ID string into a tuple of two integers.

    Accepts any of '_', ',', or '-' as separators, e.g. '028_030',
    '28,30', or '28-30'.

    Args:
        tile_id: A string containing two integers separated by '_', ',', or '-'.

    Returns:
        A tuple of two integers (x, y).

    Raises:
        LdnError: If the tile ID does not split into exactly two integers.
    """
    parts = [int(i) for i in re.split(r"[,\-_]", tile_id)]
    if len(parts) != 2:
        raise LdnError(f"Tile ID must split into 2 integers, got {parts} from '{tile_id}'")
    return parts[0], parts[1]


def get_public_https_prefix(bucket: str) -> str:
    """Return the public HTTPS URL prefix for a given bucket.

    Used for STAC hrefs and other public HTTP contexts (e.g. geoparquet URLs).

    Args:
        bucket: The S3 bucket name or custom domain.

    Returns:
        A public HTTPS URL prefix string.
    """
    if SOURCE_COOP_PUBLIC_URL:
        return SOURCE_COOP_PUBLIC_URL
    if "." in bucket:
        return f"https://{bucket}"
    return f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}"


def get_collection_url_root(
    bucket: str,
    owner: str,
    sensor: str,
    dataset_id: str,
) -> str:
    """Return the collection URL root for STAC metadata.

    Handles three bucket styles:
    - Source.Coop (e.g. 'us-west-2.opendata.source.coop'): uses the public Source.Coop HTTPS URL.
    - Custom-domain bucket (e.g. 'data.ldn.auspatious.com'): uses 'https://{bucket}'.
    - Standard S3 bucket (e.g. 'dep-public-staging'): uses the regional S3 endpoint URL.

    Args:
        bucket: The S3 bucket name or custom domain.
        owner: The owner prefix (e.g. 'dep', 'ci').
        sensor: The sensor string (e.g. 'ls').
        dataset_id: The dataset ID (e.g. 'geomad').

    Returns:
        A public HTTPS URL string suitable for use as a STAC collection URL root.
    """
    base = get_public_https_prefix(bucket)
    return f"{base}/#{owner}_{sensor}_{dataset_id}/"


def get_full_path_prefix(bucket: str) -> str:
    """Return the path prefix rasterio should use to read back written files.

    Handles three bucket styles:
    - Source.Coop (e.g. 'us-west-2.opendata.source.coop'): returns the public
      Source.Coop HTTPS URL since files are publicly readable without auth.
    - Custom-domain bucket (e.g. 'data.ldn.auspatious.com'): returns 's3://{bucket}'
      so rasterio uses boto3 credentials to authenticate.
    - Standard S3 bucket (e.g. 'dep-public-staging'): returns 's3://{bucket}'
      so rasterio uses boto3 credentials to authenticate.

    Args:
        bucket: The S3 bucket name or custom domain.

    Returns:
        A URL prefix string suitable for rasterio to open files.
    """
    if SOURCE_COOP_PUBLIC_URL:
        return SOURCE_COOP_PUBLIC_URL
    return f"s3://{bucket}"


def get_s3_mosaic_write_path(
    bucket: str,
    dataset_id: str,
    version: str,
    source_coop_prefix: str | None = None,
) -> str:
    """Return the S3 write path prefix for a given dataset.

    Handles Source.Coop (includes prefix) and standard/custom-domain buckets.

    Args:
        bucket: The S3 bucket name or custom domain.
        dataset_id: The dataset ID (e.g. 'geomad').
        version: Version string.
        source_coop_prefix: Source.Coop prefix if applicable, else None.

    Returns:
        S3 path string e.g. 's3://us-west-2.opendata.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1'
        or 's3://data.ldn.auspatious.com/ls_geomad/0-2-1'
    """
    combined_short = dataset_prefix(None, dataset_id)
    if source_coop_prefix:
        return f"s3://{bucket}/{source_coop_prefix}/{combined_short}/{version}"
    return f"s3://{bucket}/{combined_short}/{version}"
