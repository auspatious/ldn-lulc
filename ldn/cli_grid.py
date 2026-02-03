import logging
from typing import Literal

import typer
from ldn.utils import ALL_COUNTRIES, NON_DEP_COUNTRIES

cli_grid_app = typer.Typer()


@cli_grid_app.command("list-countries")
def list_countries(set: Literal["all", "non_dep", "dep"] = "all") -> dict:
    """List all unique SIDS and DEP countries as a dict of name: code."""
    logging.info(
        "Listing all 'Small Island Developing States (SIDS)' and 'Digital Earth Pacific (DEP)' countries."
    )
    set_map = {
        "all": ALL_COUNTRIES,
        "non_dep": NON_DEP_COUNTRIES,
        "dep": {k: v for k, v in ALL_COUNTRIES.items() if k not in NON_DEP_COUNTRIES},
    }
    # TODO: There are no country code duplicates/conflicts in this data, but if expanding the use of this function, check for country code conflicts too.
    sorted_combined = dict(sorted(set_map[set].items()))  # Sort by country name
    logging.info(f"Total unique countries from both datasets: {len(sorted_combined)}")
    logging.info(sorted_combined)
    return sorted_combined
