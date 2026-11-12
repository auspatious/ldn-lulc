import logging
from typing import Annotated, Literal

import geopandas as gpd
import typer
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES

from ldn.grids import get_grid_tiles as _get_grid_tiles
from ldn.utils import ALL_COUNTRIES, NON_DEP_COUNTRIES

cli_grid_app = typer.Typer()
logger = logging.getLogger(__name__)


@cli_grid_app.command()
def get_grid_tiles(
    format: Literal["list", "gdf"] = "list",
    grids: Literal["all", "pacific", "non-pacific"] = "all",
    overwrite: Annotated[bool, typer.Option()] = False,
) -> gpd.GeoDataFrame | list[tuple[tuple[int, int], str]]:
    return _get_grid_tiles(format, grids, overwrite)


@cli_grid_app.command()
def list_countries(grids: Literal["all", "pacific", "non-pacific"] = "all") -> dict:
    """List all unique SIDS and DEP countries as a dict of name: code. Depends on the grid(s) specified."""
    countries = {"all": ALL_COUNTRIES, "non-pacific": NON_DEP_COUNTRIES, "pacific": DEP_COUNTRIES_AND_CODES}[grids]
    countries = dict(sorted(countries.items()))  # Sort by country name
    logger.info(f"{len(countries)} countries in {grids} grid(s): {countries}")
    return countries
