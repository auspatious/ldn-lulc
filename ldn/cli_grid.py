import logging

import typer

cli_grid_app = typer.Typer()

# https://en.wikipedia.org/wiki/Small_Island_Developing_States
SIDS_COUNTRIES = [
    # Caribbean
    "Anguilla",
    "Antigua and Barbuda",
    "Aruba",
    "Bahamas",
    "Barbados",
    "Belize",
    "Bermuda",
    "British Virgin Islands",
    "Cayman Islands",
    "Cuba",
    "Curaçao",
    "Dominica",
    "Dominican Republic",
    "Grenada",
    "Guadeloupe",
    "Guyana",
    "Haiti",
    "Jamaica",
    "Martinique",
    "Montserrat",
    "Puerto Rico",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Sint Maarten",
    "Suriname",
    "Trinidad and Tobago",
    "Turks and Caicos Islands",
    "U.S. Virgin Islands",
    # Pacific
    "American Samoa",
    "Cook Islands",
    "Fiji",
    "French Polynesia",
    "Guam",
    "Kiribati",
    "Marshall Islands",
    "Micronesia",
    "Nauru",
    "New Caledonia",
    "Niue",
    "Northern Mariana Islands",
    "Palau",
    "Papua New Guinea",
    "Samoa",
    "Solomon Islands",
    "Timor-Leste",
    "Tonga",
    "Tuvalu",
    "Vanuatu",

    # Africa, Indian Ocean, Mediterranean, South China Sea (AIMS)
    "Cape Verde",
    "Comoros",
    "Guinea-Bissau",
    "Maldives",
    "Mauritius",
    "São Tomé and Príncipe",
    "Seychelles",
    "Singapore",	
]
# https://github.com/digitalearthpacific/dep-tools/blob/main/dep_tools/grids.py#L23
DEP_COUNTRIES = [
    "American Samoa",
    "Cook Islands",
    "Fiji",
    "French Polynesia",
    "Guam",
    "Kiribati",
    "Marshall Islands",
    "Micronesia",
    "Nauru",
    "New Caledonia",
    "Niue",
    "Northern Mariana Islands",
    "Palau",
    "Papua New Guinea",
    "Pitcairn Islands",
    "Solomon Islands",
    "Samoa",
    "Tokelau",
    "Tonga",
    "Tuvalu",
    "Vanuatu",
    "Wallis and Futuna",
]
# TODO: Maybe we need the country codes as well as the names.
# DEP_COUNTRIES_AND_CODES = {
#     "American Samoa": "ASM",
#     "Cook Islands": "COK",
#     "Fiji": "FJI",
#     "French Polynesia": "PYF",
#     "Guam": "GUM",
#     "Kiribati": "KIR",
#     "Marshall Islands": "MHL",
#     "Micronesia": "FSM",
#     "Nauru": "NRU",
#     "New Caledonia": "NCL",
#     "Niue": "NIU",
#     "Northern Mariana Islands": "MNP",
#     "Palau": "PLW",
#     "Papua New Guinea": "PNG",
#     "Pitcairn Islands": "PCN",
#     "Solomon Islands": "SLB",
#     "Samoa": "WSM",
#     "Tokelau": "TKL",
#     "Tonga": "TON",
#     "Tuvalu": "TUV",
#     "Vanuatu": "VUT",
#     "Wallis and Futuna": "WLF",
# }

@cli_grid_app.command("list-countries")
def list_countries() -> list[str]:
    logging.info("Listing all Small Island Developing States (SIDS) and Digital Earth Pacific (DEP) countries.")
    count_deps = len(DEP_COUNTRIES)
    count_sids = len(SIDS_COUNTRIES)
    logging.info(f"Number of SIDS countries: {count_sids}")
    logging.info(f"Number of DEP countries: {count_deps}")
    countries_set = set(SIDS_COUNTRIES + DEP_COUNTRIES)
    countries_list = list(countries_set)
    countries_list.sort()
    logging.info(f"Total combined countries: {len(countries_list)}, ({count_sids + count_deps} before deduplication)")
    logging.info(f"Countries list {countries_list}")
    return countries_list
