import logging
from typing import Literal

import typer
from ldn.utils import ALL_COUNTRIES, NON_DEP_COUNTRIES
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

cli_grid_app = typer.Typer()


@cli_grid_app.command("list-countries")
def list_countries(grids: Literal["both", "ci", "dep"] = "both") -> dict:
    """List all unique SIDS and DEP countries as a dict of name: code. Depends on the grid(s) specified."""
    if grids not in ["both", "ci", "dep"]:
        logging.error(f"Invalid grid option: {grids}. Must be one of 'both', 'ci', or 'dep'.")
        raise ValueError(f"Invalid grid option: {grids}. Must be one of 'both', 'ci', or 'dep'.")
    if grids == "both":
        logging.info("Listing all 'Small Island Developing States (SIDS)' and 'Digital Earth Pacific (DEP)' countries for both grids.")
    elif grids == "ci":
        logging.info("Listing 'Small Island Developing States (SIDS)' countries for CI grid.")
    elif grids == "dep":
        logging.info("Listing 'Digital Earth Pacific (DEP)' countries for DEP grid.")

    set_map = {
        "both": ALL_COUNTRIES,
        "ci": NON_DEP_COUNTRIES,
        "dep": DEP_COUNTRIES_AND_CODES,
    }
    sorted_combined = dict(sorted(set_map[grids].items()))  # Sort by country name
    logging.info(f"Total unique countries from both datasets: {len(sorted_combined)}")
    logging.info(sorted_combined)
    return sorted_combined
