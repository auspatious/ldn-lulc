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
    bucket = BUCKET  # TODO: Make this a param?
    owner = owner_for_region(region, product_owner=product_owner)
    prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
    if SOURCE_COOP_PUBLIC_URL:
        return f"{SOURCE_COOP_PUBLIC_URL}/{SOURCE_COOP_PREFIX_GEOMAD}/{prefix}/{ver}/{prefix}.parquet"
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
