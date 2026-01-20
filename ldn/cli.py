import logging
import sys

import typer

from ldn import get_version
from ldn.cli_grid import cli_grid_app

app = typer.Typer()

# All files will inherit this logging configuration so we only write once
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(module)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)

# Add the subcommands
app.add_typer(cli_grid_app, name="grid", help="Commands for working with the ODC Geo Grid.")

# Work for version and --version
@app.command()
def version() -> None:
    """Echo the version of the software."""

    version = get_version()
    typer.echo(version)

    return


if __name__ == "__main__":
    app()
