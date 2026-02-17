import logging
from typing import Literal

import typer
from ldn.utils import ALL_COUNTRIES, NON_DEP_COUNTRIES
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

cli_grid_app = typer.Typer()

@cli_grid_app.command("list-countries")
def list_countries(grids: Literal["all", "pacific", "non-pacific"] = "all") -> dict:
    """List all unique SIDS and DEP countries as a dict of name: code. Depends on the grid(s) specified."""
    if grids not in ["all", "pacific", "non-pacific"]:
        logging.error(f"Invalid grid option: {grids}. Must be one of 'all', 'pacific', or 'non-pacific'.")
        raise ValueError(f"Invalid grid option: {grids}. Must be one of 'all', 'pacific', or 'non-pacific'.")
    if grids == "all":
        logging.info("Listing all 'Small Island Developing States (SIDS)' and 'Digital Earth Pacific (DEP)' countries for all grids.")
    elif grids == "non-pacific":
        logging.info("Listing 'Small Island Developing States (SIDS)' countries for non-Pacific grid.")
    elif grids == "pacific":
        logging.info("Listing 'Digital Earth Pacific (DEP)' countries for Pacific grid.")
    set_map = {
        "all": ALL_COUNTRIES,
        "non-pacific": NON_DEP_COUNTRIES,
        "pacific": DEP_COUNTRIES_AND_CODES,
    }
    sorted_combined = dict(sorted(set_map[grids].items()))  # Sort by country name
    logging.info(f"Total unique countries from {grids} dataset(s): {len(sorted_combined)}")
    logging.info(sorted_combined)
    return sorted_combined
