import logging
import sys

import boto3
from dep_tools.namers import S3ItemPath
from dep_tools.aws import object_exists
from dep_tools.searchers import PystacSearcher
from dep_tools.loaders import OdcLoader
from typing_extensions import Annotated
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task
from dep_tools.writers import AwsDsCogWriter
from odc.stac import configure_s3_access

import json

from dep_tools.exceptions import EmptyCollectionError
from dask.distributed import Client as DaskClient

from ldn.geomad import (
    GeoMADLandsatProcessor,
    LANDSAT_SCALE,
    LANDSAT_OFFSET,
    USGS_CATALOG,
    USGS_COLLECTION,
    LANDSAT_BANDS,
)
from ldn.grids import get_all_tiles
import typer

from ldn import get_version
from ldn.cli_grid import cli_grid_app
from ldn.grids import get_gridspec

app = typer.Typer()

# All files will inherit this logging configuration so we only write once
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(module)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)

# Add the subcommands
app.add_typer(
    cli_grid_app, name="grid", help="Commands for working with the ODC Geo Grid."
)

# Work for version and --version
@app.command()
def version() -> None:
    """Echo the version of the software."""

    version = get_version()
    typer.echo(version)

    return


if __name__ == "__main__":
    app()


@app.command()
def print_tasks(
    years: Annotated[str, typer.Option()] = "2025",
) -> None:
    """Print all tasks for given years."""
    # Parse the year string, making comma separated a list
    # and - separated a range
    years_list = []
    if "," in years:
        years_list = years.split(",")
    elif "-" in years:
        start_year, end_year = map(int, years.split("-"))
        years_list = [str(y) for y in range(start_year, end_year + 1)]
    else:
        years_list = [years]

    assert len(years_list) > 0, "No years provided"
    assert all(y.isdigit() for y in years_list), "Years must be integers"

    tasks = []
    for year in years_list:
        for tile in get_all_tiles():
            tasks.append({"id": "_".join(str(i) for i in tile), "year": year})

    typer.echo(json.dumps(tasks, indent=2))

    return


@app.command()
def geomad(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    bucket: Annotated[str, typer.Option()] = "data.ldn.auspatious.com",
    overwrite: Annotated[bool, typer.Option()] = False,
    decimated: Annotated[bool, typer.Option()] = False,
    all_bands: Annotated[bool, typer.Option()] = True,
    memory_limit: Annotated[str, typer.Option()] = "10GB",
    n_workers: Annotated[int, typer.Option()] = 2,
    threads_per_worker: Annotated[int, typer.Option()] = 16,
    xy_chunk_size: Annotated[int, typer.Option()] = 2048,
    geomad_threads: Annotated[int, typer.Option()] = 10,
) -> None:
    """Run GeoMAD processing on Landsat data.
    
    Example command is:

    ldn geomad --tile-id 136_142 --year 2025 --version 0.0.0 \
        --overwrite \
        --decimated \
        --no-all-bands
    """
    typer.echo(
        f"Running GeoMAD processing for tile {tile_id}, year {year}, version {version}"
    )

    # Fixed variables
    sensor = "ls"

    # Set up variables and check
    tile_index = tuple(map(int, tile_id.split("_")))
    grid = get_gridspec()
    geobox = grid.tile_geobox(tile_index)

    if bucket == "data.ldn.auspatious.com":
        full_path_prefix = "https://data.ldn.auspatious.com"

    if decimated:
        typer.echo("Warning, using decimated bands for testing purposes.")
        geobox = geobox.zoom_out(10)

    # Configure for dask and reading data
    _ = configure_s3_access(requester_pays=True)
    # Configure for checking item existence
    client = boto3.client("s3")

    # Check if we've done this tile before
    itempath = S3ItemPath(
        prefix="ausp",
        bucket=bucket,
        sensor=sensor,
        dataset_id="geomad",
        version=version,
        time=year,
        full_path_prefix=full_path_prefix,
    )
    stac_document = itempath.stac_path(tile_index, absolute=True)
    stac_key = itempath.stac_path(tile_index, absolute=False)

    # If we don't want to overwrite, and the destination file already exists, skip it
    if not overwrite and object_exists(bucket, stac_key, client=client):
        typer.echo(f"Item already exists at {stac_document}")
        # This is an exit with success
        raise typer.Exit()
    else:
        if not overwrite:
            typer.echo(f"Item does not exist at {stac_document}, proceeding to write.")

    search_kwargs = {"query": {"landsat:collection_category": {"in": ["T1"]}}}
    load_kwargs = {}

    # Searcher finds STAC Items
    # TODO: Set up fallback for if there's not enough T1 data
    searcher = PystacSearcher(
        catalog=USGS_CATALOG,
        collections=[USGS_COLLECTION],
        datetime=year,
        **search_kwargs,
    )

    # Loader loads the data from STAC Items
    loader = OdcLoader(
        bands=LANDSAT_BANDS if all_bands else ["red", "green", "blue", "qa_pixel"],
        chunks={"x": xy_chunk_size, "y": xy_chunk_size, "time": 1},
        groupby="solar_day",
        fail_on_error=False,
        **load_kwargs,
    )

    # AWS Writer, to write results
    writer = AwsDsCogWriter(itempath, write_multithreaded=True)

    # Metadata creator
    stac_creator = StacCreator(
        collection_url_root="https://data.ldn.auspatious.com/#ausp_ls_geomad/",
        itempath=itempath,
        with_raster=True,
    )

    processor = GeoMADLandsatProcessor(
        geomad_options=dict(
            work_chunks=(100, 100),
            num_threads=geomad_threads,
            maxiters=100,
            scale=LANDSAT_SCALE,
            offset=LANDSAT_OFFSET,
            nodata=0,
        ),
        min_timesteps=5,
        drop_vars=["qa_pixel"],
    )

    try:
        with DaskClient(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
        ):
            paths = Task(
                itempath=itempath,
                id=tile_index,
                area=geobox,
                searcher=searcher,
                loader=loader,
                processor=processor,
                writer=writer,
                stac_creator=stac_creator,
            ).run()
            typer.echo(f"Wrote {len(paths)} files...")
    except EmptyCollectionError:
        typer.echo("No items found for this tile")
        raise typer.Exit()  # Exit with success
    except Exception as e:
        typer.echo(f"Failed to process with error: {e}")
        raise typer.Exit(code=1)

    typer.echo(f"Finished writing to {stac_document}")

    return
