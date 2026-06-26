import logging
import os
import re
from typing import Literal

from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES
from pystac import ItemCollection
from rustac import read_sync
from rustac import store as rustac_store

from ldn.aws import aws_session, credential_provider

logger = logging.getLogger(__name__)


# For BUCKET
# Keep this bucket. It is important.
def get_env_var(name: str) -> str:
    """Return the value of the environment variable."""
    value = os.environ.get(name)
    if not value:
        raise LdnError(f"Requested '{name}' environment variable is not set.")
    logger.info(f"Got environment variable '{name}' = '{value}'")
    return value


def is_bucket_source_coop(bucket: str) -> bool:
    """Determine if a bucket is a Source.Coop bucket based on its name."""
    return bucket.endswith("source.coop")


SOURCE_COOP_URL = "https://data.source.coop"  # Public URL. Just used for STAC metadata so users can read it easily.
SOURCE_COOP_PREFIX_GEOMAD = "auspatious/geomad-sids"
SOURCE_COOP_PREFIX_LULC = "auspatious/lulc-sids"


# Our custom exception class for the project. Good for filtering errors in processing.
class LdnError(Exception):
    """Base exception for the ldn-lulc project."""


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

GEOMAD_VERSION = "0-3-0"  # Will write 0-3-0 for DEP prod.
LULC_VERSION = "0-0-9"
MODEL_VERSION = "0-0-9"
TRAINING_DATA_VERSION = "0-0-9"

PACIFIC_OWNER = "dep"
NON_PACIFIC_OWNER = "ci"

SENSOR = "ls"
GEOMAD_DATASET_ID = "geomad"
LULC_DATASET_ID = "lulc"

LS7_YEAR_THRESHOLD = 2012
TRAINING_DATA_YEAR = "2020"
CLASS_ATTR = "lulc"  # For training data creation.
WGS84 = "EPSG:4326"


def owner_for_region(
    region: Literal["pacific", "non-pacific"],
    product_owner: str | None = None,
) -> str:
    """Return the short owner prefix for a given region (e.g. 'dep' or 'ci').

    If product_owner is set, it overrides the region-based lookup.
    """
    if product_owner is not None:
        return product_owner
    return PACIFIC_OWNER if region == "pacific" else NON_PACIFIC_OWNER


def dataset_prefix(owner: str | None, sensor: str, dataset_id: str) -> str:
    """Build the full dataset prefix from owner, sensor, and dataset_id (owner optional).

    Args:
        owner: Short owner prefix (e.g. "dep" or "ci").
        sensor: Sensor identifier (e.g. "ls").
        dataset_id: Dataset identifier (e.g. "geomad" or "lulc").

    Returns:
        Full prefix like "dep_ls_geomad" or "ci_ls_lulc" or "ls_geomad" if owner is None.
    """
    if owner:
        return f"{owner}_{sensor}_{dataset_id}"
    else:
        return f"{sensor}_{dataset_id}"


def get_stac_geoparquet_key(
    bucket: str,
    product_owner: str | None,
    sensor: str,
    dataset: Literal["geomad", "lulc"],
    version: str,
) -> str:
    """Return the S3 key for the STAC-Geoparquet file (no bucket etc.).

    Args:
        dataset: The dataset type (e.g. 'geomad' or 'lulc').
        version: Version string.
        source_coop_prefix: Source.Coop prefix if applicable, else None.
        single_region: Whether to use the single region prefix
        (e.g. 'dep_ls_geomad') or the generic prefix (e.g. 'ls_geomad').
        region: The region for which to build the prefix ('pacific' or 'non-pacific').

    Returns:
        S3 key string e.g. 'auspatious/geomad-sids/ls_geomad/0-3-0/ls_geomad.parquet'
        or 'ls_geomad/0-3-0/ls_geomad.parquet' for a standard bucket.
    """
    filename = dataset_prefix(product_owner, sensor, dataset) + ".parquet"
    prefix_with_sc = build_prefix(bucket, product_owner, sensor, dataset, version)

    return f"{prefix_with_sc}/{filename}"


def get_write_url_base(bucket: str) -> str:
    """Return the URL base for writing files to S3, which may differ for Source.Coop vs standard buckets."""
    return f"https://s3.{aws_session.region_name}.amazonaws.com/{bucket}"


def get_stac_geoparquet_url(bucket: str, key: str) -> str:
    """Return the URL to the GeoMAD STAC-Geoparquet file for use with rustac/DuckDB.

    Args:
        bucket: The S3 bucket name or custom domain.
        key: The S3 key for the STAC-Geoparquet file.

    Returns:
        URL to the STAC-Geoparquet file. e.g. https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/auspatious/geomad-sids/ls_geomad/0-3-0/ls_geomad.parquet
    """
    # https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/auspatious/geomad-sids/ls_geomad/0-3-0/ls_geomad.parquet
    # https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ls_geomad/test-integration/ls_geomad.parquet
    return f"{get_write_url_base(bucket)}/{key}"


def get_analysis_epsg(
    region: Literal["pacific", "non-pacific"],
) -> Literal["EPSG:3832", "EPSG:6933"]:
    """Return the appropriate EPSG code for analysis based on the region."""
    if region == "pacific":
        return "EPSG:3832"
    return "EPSG:6933"


def parse_years(years: str) -> list[int]:
    """Parse a years string into a list of integers.

    Accepts a comma-separated list (e.g. '2020,2021') or a range (e.g. '2010-2023').
    Ranges must be ascending (e.g. '2020-2023').
    """
    if "," in years:
        return [int(y.strip()) for y in years.split(",")]
    elif "-" in years:
        start_year, end_year = map(int, years.split("-"))
        if start_year > end_year:
            raise ValueError(f"Invalid year range: '{years}'. Start year must be <= end year.")
        return list(range(start_year, end_year + 1))
    else:
        return [int(years)]


def source_coop_prefix(dataset_id: Literal["geomad", "lulc"]) -> str:
    """Return the source.coop path prefix for a dataset"""
    if dataset_id == GEOMAD_DATASET_ID:
        return SOURCE_COOP_PREFIX_GEOMAD
    else:
        return SOURCE_COOP_PREFIX_LULC


def version_for_dataset(
    dataset: Literal["geomad", "lulc"],
    geomad_version: str,
    lulc_version: str,
) -> str:
    """Return the version string for the given dataset name."""
    return geomad_version if dataset == GEOMAD_DATASET_ID else lulc_version


def parse_tile_id(tile_id: str) -> tuple[int, int]:
    """Parse a tile ID string into a tuple of two integers.

    Accepts any of '_', ',', or '-' as separators, e.g. '028_030',
    '28,30', or '28-30'.

    Args:
        tile_id: A string containing two integers separated by '_', ',', or '-'.

    Returns:
        A tuple of two integers (x, y). These are not padded with zeros because they are integers, not strings.

    Raises:
        LdnError: If the tile ID does not split into exactly two integers.
    """
    parts = [int(i) for i in re.split(r"[,\-_]", tile_id)]
    if len(parts) != 2:
        raise LdnError(f"Tile ID must split into 2 integers, got {parts} from '{tile_id}'")
    return parts[0], parts[1]


def get_public_https_base(bucket: str) -> str:
    """Return the public HTTPS URL base for a given bucket.

    Used for STAC hrefs and other public HTTP contexts (e.g. geoparquet URLs).

    Supports three bucket styles:
    - Source.Coop buckets (e.g. 'us-west-2.opendata.source.coop'): returns the public Source.Coop HTTPS URL
    since files are publicly readable without auth.
    - Custom-domain buckets (e.g. 'data.ldn.auspatious.com'): returns 'https://{bucket}'.
    - Standard S3 buckets (e.g. 'dep-public-staging'): returns the regional S3 endpoint URL.

    Args:
        bucket: The S3 bucket name or custom domain.

    Returns:
        A public HTTPS URL base string.
    """
    _is_bucket_source_coop = is_bucket_source_coop(bucket)
    if _is_bucket_source_coop:
        # "https://data.source.coop"
        return SOURCE_COOP_URL
    if "." in bucket:
        # e.g. "data.ldn.auspatious.com"
        return f"https://{bucket}"
    # e.g. "dep-public-staging"
    return get_write_url_base(bucket)


def get_collection_url_root(
    bucket: str,
    owner: str | None,
    sensor: str,
    dataset: Literal["geomad", "lulc"],
    version: str,
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
        dataset: The dataset ID (e.g. 'geomad').
        version: The dataset version string (e.g. '0-3-0').

    Returns:
        A public HTTPS URL string suitable for use as a STAC collection URL root.
    """
    public_url_root = get_public_https_base(bucket)

    prefix = build_prefix(bucket, owner, sensor, dataset, version)

    # TODO: Validate this. The default in DEP tools is https://stac.staging.digitalearthpacific.io/collections
    return f"{public_url_root}/collections/{prefix}/"  # Version in root?


def get_full_path_prefix(bucket: str) -> str:
    """Return the path prefix rasterio should use to read back written files.

    Handles three bucket styles:
    - Source.Coop (e.g. 'us-west-2.opendata.source.coop'): returns the public
      Source.Coop HTTPS URL since files are publicly readable without auth.
    - Custom-domain bucket (e.g. 'data.ldn.auspatious.com') and Standard S3 bucket (e.g. 'dep-public-staging'):
     returns 's3://{bucket}' so rasterio uses boto3 credentials to authenticate.

    Args:
        bucket: The S3 bucket name or custom domain.

    Returns:
        A URL prefix string suitable for rasterio to open files.
    """
    _is_bucket_source_coop = is_bucket_source_coop(bucket)
    if _is_bucket_source_coop:
        return SOURCE_COOP_URL
    return f"s3://{bucket}"


def load_stac_geoparquet_features(bucket: str, prefix: str) -> ItemCollection:
    """Load all STAC items from a STAC-geoparquet file.

    Args:
        bucket: The S3 bucket name or custom domain.
        prefix: The prefix to the STAC-Geoparquet file e.g. 'dep_ls_geomad/0-3-0/dep_ls_geomad.parquet'
        or 'auspatious/geomad-sids/ls_geomad/0-3-0/ls_geomad.parquet'.

    Returns:
        ItemCollection of STAC items.
    """
    store = rustac_store.S3Store(bucket, credential_provider=credential_provider)

    feature_collection = read_sync(prefix, store=store)
    features = feature_collection.get("features", [])
    if not features:
        raise LdnError("No items found.")
    logger.info(f"Loaded {len(features)} features from {prefix}")
    return ItemCollection(features)


def build_prefix(
    bucket: str, product_owner: str | None, sensor: str, dataset: Literal["geomad", "lulc"], version: str
) -> str:
    """Build the prefix for a dataset. Does not include the bucket or key (filename).

    Args:
    product_owner: The owner prefix (e.g. 'dep', 'ci' or 'example_override_value').

    Returns:
        e.g. "dep_ls_geomad/0-3-0" or "ls_geomad/0-3-0"
    """
    prefix = f"{dataset_prefix(product_owner, sensor, dataset)}/{version}"
    if is_bucket_source_coop(bucket):
        prefix = f"{source_coop_prefix(dataset)}/{prefix}"

    return prefix
