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

TEST_TILES = [
    ("058_043", "pacific", {"Kiribati": "KIR"}),
    ("063_020", "pacific", {"Fiji": "FJI"}),
    ("066_022", "pacific", {"Fiji": "FJI"}),
    ("119_126", "non-pacific", {"Belize": "BLZ"}),
    ("152_110", "non-pacific", {"Suriname": "SUR"}),
    ("185_125", "non-pacific", {"Cabo Verde": "CPV"}),  # Cape?
    ("251_088", "non-pacific", {"Comoros": "COM"}),
    ("312_105", "non-pacific", {"Singapore": "SGP"}),
    ("312_106", "non-pacific", {"Singapore": "SGP"}),
    ("089_016", "pacific", {"Cook Islands": "COK"}),
]

GEOMAD_VERSION = "0-0-4a"
PREDICTION_VERSION = "0-0-3"

training_data_year = "2020"

class_attr = "lulc"

wgs84 = "EPSG:4326"


def get_analysis_epsg(
    region: Literal["pacific", "non-pacific"],
) -> Literal["EPSG:3832", "EPSG:6933"]:
    if region == "pacific":
        return "EPSG:3832"
    else:
        return "EPSG:6933"
