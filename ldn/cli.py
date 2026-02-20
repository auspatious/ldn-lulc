import logging
import sys
import warnings

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
from rasterio.errors import NotGeoreferencedWarning
from typing import Literal

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
from ldn.grids import get_grid_tiles
import typer

from ldn import get_version
from ldn.cli_grid import cli_grid_app
from ldn.grids import get_gridspec

app = typer.Typer()

# Configure logging so CLI output shows only ldn logs by default.
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(module)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"

root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(logging.WARNING)

ldn_handler = logging.StreamHandler(sys.stderr)
ldn_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))

logger = logging.getLogger("ldn")
logger.handlers.clear()
logger.addHandler(ldn_handler)
logger.setLevel(logging.INFO)
logger.propagate = False

logger = logging.getLogger(__name__)

# Reduce known noisy third-party warnings while keeping actionable logs visible.
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"odc\.algo\._masking",
    message=r"`binary_dilation` is deprecated since version 0\.26.*",
)
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"odc\.algo\._masking",
    message=r"`binary_erosion` is deprecated since version 0\.26.*",
)
warnings.filterwarnings(
    "ignore",
    category=NotGeoreferencedWarning,
    module=r"rasterio\.warp",
)
logging.getLogger("rasterio._err").setLevel(logging.ERROR)
logging.getLogger("dask").setLevel(logging.ERROR)
logging.getLogger("distributed").setLevel(logging.ERROR)
logging.getLogger("distributed.shuffle").setLevel(logging.ERROR)
logging.getLogger("distributed.shuffle._scheduler_plugin").setLevel(logging.ERROR)

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
    years: Annotated[str, typer.Option()],
    grids: Annotated[Literal["all", "pacific", "non-pacific"], typer.Option()] = "all",
) -> None:
    """Print all tasks for given years for either all grids, or just the Pacific or non-Pacific grid."""
    logger.info(f"Generating tasks for years: {years} and grids: {grids}")

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

    tiles = get_grid_tiles(format="list", grids=grids, overwrite=False)

    logger.info(
        f"Number of tasks: {len(years_list) * len(tiles)} (years: {len(years_list)}, tiles: {len(tiles)})"
    )

    tasks = []
    for year in years_list:
        # get_grid_tiles handles all (Pacific and non-Pacific grids) or just one.
        for tile in tiles:
            tasks.append(
                {
                    "id": "_".join(str(i) for i in tile[0]),
                    "year": year,
                    "region": tile[1],
                }
            )

    tasks_json_str = json.dumps(tasks, indent=2)
    with open("tasks.json", "w") as f:
        f.write(tasks_json_str)

    typer.echo(tasks_json_str)
    return


@app.command()
def geomad(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    region: Annotated[Literal["pacific", "non-pacific"], typer.Option()],
    bucket: Annotated[str, typer.Option()] = "data.ldn.auspatious.com",
    overwrite: Annotated[bool, typer.Option()] = False,
    decimated: Annotated[bool, typer.Option()] = False,
    include_shadow: Annotated[bool, typer.Option()] = False,
    ls7_buffer_years: Annotated[int, typer.Option()] = 1,
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
        --no-all-bands \
        --region pacific
    """
    info = (
        f"Running GeoMAD processing for tile {tile_id}, year {year}, version {version},"
        f" region {region} with overwrite={overwrite}, decimated={decimated},"
        f" all_bands={all_bands}, memory_limit={memory_limit}, n_workers={n_workers},"
        f" threads_per_worker={threads_per_worker}, xy_chunk_size={xy_chunk_size}, "
        f"geomad_threads={geomad_threads}, include_shadow={include_shadow}"
    )
    typer.echo(info)
    if region not in ["pacific", "non-pacific"]:
        raise ValueError(f"Invalid region: {region}. Must be 'pacific' or 'non-pacific'.")

    year_int = int(year)
    search_year = year
    # If we're in the LS7 era, use a buffered window of data
    if year_int <= 2012:
        year_start = year_int - ls7_buffer_years
        year_end = year_int + ls7_buffer_years
        search_year = f"{year_start}/{year_end}"
        typer.echo(f"Using {ls7_buffer_years}-year buffered window for LS7 era: {search_year}")

    # For now, if we're in the Pacific, use both T1 and T2 data
    # This may be necessary in other places too
    search_kwargs = {"query": {"landsat:collection_category": {"in": ["T1"]}}}
    if region == "pacific":
        if year_int <= 2012:
            # Searching for nothing gives us everything
            typer.echo("Using both T1 and T2 data for Pacific for LS7 era")
            search_kwargs == {}

    # Fixed variables
    sensor = "ls"
    dataset_id = "geomad"

    # Set up variables and check
    tile_index = tuple(map(int, tile_id.split("_")))

    grid = get_gridspec(region=region)
    geobox = grid.tile_geobox(tile_index)

    if not bucket.startswith("https://"):
        full_path_prefix = "https://data.ldn.auspatious.com"

    if decimated:
        typer.echo("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    # Configure for dask and reading data
    _ = configure_s3_access(requester_pays=True)
    # Configure for checking item existence
    client = boto3.client("s3")

    prefix = "ci" if region == "non-pacific" else "dep"

    # Check if we've done this tile before
    itempath = S3ItemPath(
        prefix=prefix,
        bucket=bucket,
        sensor=sensor,
        dataset_id=dataset_id,
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
            typer.echo(f"Item does not exist at {stac_document}, processing tile.")

    load_kwargs = {}

    # Searcher finds STAC Items
    # TODO: Set up fallback for if there's not enough T1 data
    searcher = PystacSearcher(
        catalog=USGS_CATALOG,
        collections=[USGS_COLLECTION],
        datetime=search_year,
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
        collection_url_root=f"https://data.ldn.auspatious.com/#{prefix}_{sensor}_{dataset_id}/",
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
        mask_clouds_kwargs={
            "filters": [("dilation", 3), ("erosion", 2)],
            "include_shadow": include_shadow
        },
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
