import logging

import typer
from ldn.utils import ALL_COUNTRIES

cli_grid_app = typer.Typer()


@cli_grid_app.command("list-countries")
def list_countries() -> dict:
    """List all unique SIDS and DEP countries as a dict of name: code."""
    logging.info(
        "Listing all 'Small Island Developing States (SIDS)' and 'Digital Earth Pacific (DEP)' countries."
    )
    # TODO: There are no country code duplicates/conflicts in this data, but if expanding the use of this function, check for country code conflicts too.
    sorted_combined = dict(sorted(ALL_COUNTRIES.items()))  # Sort by country name
    logging.info(f"Total unique countries from both datasets: {len(sorted_combined)}")
    logging.info(sorted_combined)
    return sorted_combined
