import logging
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
NON_DEP_COUNTRIES = {
    k: v
    for k, v in SIDS_COUNTRIES_AND_CODES.items()
    if k not in DEP_COUNTRIES_AND_CODES
}

# TODO: Define the 5 countries for the pacific that will be used to make training data.
# for these countries: xyz.
# give me 5 that are indicative of different environments e.g. forest, atoll, volcanic, elevated, urban, beach, wetland, grassland, cropland, etc, Give me more if more than 5 are needed
PACIFIC_TRAINING_TILES = [
    ("028_030", "pacific", {"Papua New Guinea": "PNG"}),
    ("058_043", "pacific", {"Kiribati": "KIR"}),
    ("063_020", "pacific", {"Fiji": "FJI"}),
    ("076_024", "pacific", {"American Samoa": "ASM"}),
    ("089_016", "pacific", {"Cook Islands": "COK"}),
]

# TEST_TILES = [
#     ("058_043", "pacific", {"Kiribati": "KIR"}),
#     ("063_020", "pacific", {"Fiji": "FJI"}),
#     ("066_022", "pacific", {"Fiji": "FJI"}),
#     ("119_126", "non-pacific", {"Belize": "BLZ"}),
#     ("152_110", "non-pacific", {"Suriname": "SUR"}),
#     ("185_125", "non-pacific", {"Cabo Verde": "CPV"}),  # Cape?
#     ("251_088", "non-pacific", {"Comoros": "COM"}),
#     ("312_105", "non-pacific", {"Singapore": "SGP"}),
#     ("312_106", "non-pacific", {"Singapore": "SGP"}),
#     ("089_016", "pacific", {"Cook Islands": "COK"}),
# ]

# TEST_TILES_PACIFIC = [
#     # Already ran a tile for these (in TEST_TILES):
#     # Cook Islands
#     # Fiji
#     # Kiribati
#     # New ones to run:
#     # Selecting the main island of each country.
#     ("051_052", "pacific", {"Marshall Islands": "MHL"}),  # Kwajalein Atoll
#     ("040_049", "pacific", {"Micronesia": "FSM"}),  # Pohnpei
#     ("050_041", "pacific", {"Nauru": "NRU"}),
#     ("077_019", "pacific", {"Niue": "NIU"}),
#     ("013_050", "pacific", {"Palau": "PLW"}),
#     ("028_030", "pacific", {"Papua New Guinea": "PNG"}),
#     ("075_025", "pacific", {"Samoa": "WSM"}),
#     ("042_030", "pacific", {"Solomon Islands": "SLB"}),
#     ("071_016", "pacific", {"Tonga": "TON"}),
#     ("065_031", "pacific", {"Tuvalu": "TUV"}),
#     ("052_021", "pacific", {"Vanuatu": "VUT"}),
# ]

GEOMAD_VERSION = "0-2-1"
PREDICTION_VERSION = "0-0-4"
MODEL_VERSION = "0-0-4"
TRAINING_DATA_VERSION = "0-0-4"

# Mosaic source configuration per region
PACIFIC_BUCKET = "dep-public-staging"
PACIFIC_OWNER = "dep"

NON_PACIFIC_BUCKET = "data.ldn.auspatious.com"
NON_PACIFIC_OWNER = "ci"

SENSOR = "ls"
GEOMAD_DATASET_ID = "geomad"
PREDICTION_DATASET_ID = "lulc_prediction"
AWS_REGION = "us-west-2"

training_data_year = "2020"

class_attr = "lulc"

wgs84 = "EPSG:4326"


def bucket_for_region(
    region: Literal["pacific", "non-pacific"],
    bucket_pacific: str = PACIFIC_BUCKET,
    bucket_non_pacific: str = NON_PACIFIC_BUCKET,
) -> str:
    """Return the S3 bucket for a given region."""
    return bucket_pacific if region == "pacific" else bucket_non_pacific


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
    bucket = bucket_for_region(region)
    owner = owner_for_region(region, product_owner=product_owner)
    prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
    return f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}/{prefix}/{ver}/{prefix}.parquet"


def get_geomad_item_id(
    region: Literal["pacific", "non-pacific"],
    tile_id: str,
    year: str,
    product_owner: str | None = None,
) -> str:
    """Build the STAC item ID for a GeoMAD tile.

    Args:
        region: Either "pacific" or "non-pacific".
        tile_id: Grid tile identifier (e.g. "058_043").
        year: Year string (e.g. "2020").
        product_owner: Optional override for the region-derived owner prefix.

    Returns:
        The full STAC item ID string.
    """
    owner = owner_for_region(region, product_owner=product_owner)
    prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
    return f"{prefix}_{tile_id}_{year}"


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
