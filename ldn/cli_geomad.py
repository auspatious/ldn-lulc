import logging
import sys

import boto3
from dask.distributed import KilledWorker
from dep_tools.namers import S3ItemPath
from dep_tools.aws import object_exists
from dep_tools.searchers import PystacSearcher
from dep_tools.loaders import OdcLoader
from typing_extensions import Annotated
from dep_tools.stac_utils import StacCreator
from ldn.geomad import AwsStacTask as Task
from dep_tools.writers import AwsDsCogWriter
from odc.stac import configure_s3_access
from typing import Literal

from dep_tools.exceptions import EmptyCollectionError
from dask.distributed import Client as DaskClient

from ldn.geomad import (
    GeoMADProcessor,
    LANDSAT_SCALE,
    LANDSAT_OFFSET,
    USGS_CATALOG,
    USGS_COLLECTION,
    LANDSAT_BANDS,
    InsufficientScenesError,
)
import typer

from ldn.grids import get_gridspec
from ldn.utils import (
    AWS_REGION,
    GEOMAD_DATASET_ID,
    SENSOR,
    PACIFIC_BUCKET,
    NON_PACIFIC_BUCKET,
    PACIFIC_OWNER,
    NON_PACIFIC_OWNER,
    bucket_for_region,
    owner_for_region,
)

geomad_app = typer.Typer()
logger = logging.getLogger(__name__)

EXIT_OOM = 42  # KilledWorker — retry with more resources
EXIT_SKIP = 43  # too few scenes / no items — expected, don't retry


@geomad_app.command()
def run(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    region: Annotated[Literal["pacific", "non-pacific"], typer.Option()],
    bucket_pacific: Annotated[
        str, typer.Option(help="S3 bucket for pacific data.")
    ] = PACIFIC_BUCKET,
    bucket_non_pacific: Annotated[
        str, typer.Option(help="S3 bucket for non-pacific data.")
    ] = NON_PACIFIC_BUCKET,
    owner_pacific: Annotated[
        str, typer.Option(help="Short owner prefix for pacific (e.g. 'dep').")
    ] = PACIFIC_OWNER,
    owner_non_pacific: Annotated[
        str, typer.Option(help="Short owner prefix for non-pacific (e.g. 'ci').")
    ] = NON_PACIFIC_OWNER,
    product_owner: Annotated[
        str | None, typer.Option(help="Override the region-derived owner prefix.")
    ] = None,
    overwrite: Annotated[bool, typer.Option()] = False,
    decimated: Annotated[bool, typer.Option()] = False,
    mask_shadow: Annotated[
        bool,
        typer.Option(
            help="True to mask cloud shadows, false to not mask them (leave them in). Defaults to True."
        ),
    ] = True,
    ls7_buffer_years: Annotated[
        int,
        typer.Option(
            help="Half-width of the temporal buffer for LS7 era (<=2012). E.g. 1 searches year-1 to year+1."
        ),
    ] = 1,
    all_bands: Annotated[bool, typer.Option()] = True,
    memory_limit: Annotated[str, typer.Option()] = "10GB",
    n_workers: Annotated[int, typer.Option()] = 2,
    threads_per_worker: Annotated[int, typer.Option()] = 16,
    xy_chunk_size: Annotated[int, typer.Option()] = 2048,
    geomad_threads: Annotated[int, typer.Option()] = 10,
) -> None:
    """Run GeoMAD processing on a single tile for a year.

    Searches USGS STAC for Landsat scenes covering the given tile and year,
    applies cloud masking, computes the geometric median and median absolute
    deviations (GeoMAD), and writes COG outputs to S3.

    For years in the Landsat 7 era (<=2012), a buffered temporal window
    controlled by --ls7-buffer-years is used to gather enough clear
    observations. Pacific tiles may additionally include Tier 2 data.
    """
    logger.info(
        f"tile={tile_id} year={year} version={version} region={region} overwrite={overwrite} decimated={decimated} "
        f"all_bands={all_bands} mask_shadow={mask_shadow} memory={memory_limit} workers={n_workers} threads={threads_per_worker} "
        f"chunk={xy_chunk_size} geomad_threads={geomad_threads}",
    )

    year_int = int(year)
    search_year = year
    # If we're in the LS7 era, use a buffered window of data
    if year_int <= 2012:
        year_start = year_int - ls7_buffer_years
        year_end = year_int + ls7_buffer_years
        search_year = f"{year_start}/{year_end}"
        typer.echo(
            f"Using {ls7_buffer_years}-year buffered window for LS7 era: {search_year}"
        )

    # For now, if we're in the Pacific, use both T1 and T2 data
    # This may be necessary in other places too
    search_kwargs = {"query": {"landsat:collection_category": {"in": ["T1"]}}}
    if region == "pacific":
        if year_int <= 2012:
            # Searching for nothing gives us everything
            typer.echo("Using both T1 and T2 data for Pacific for LS7 era")
            search_kwargs = {}

    # Set up variables and check
    tile_index = tuple(map(int, tile_id.split("_")))

    grid = get_gridspec(region=region)
    geobox = grid.tile_geobox(tile_index)

    # Resolve bucket and prefix based on tile region
    bucket = bucket_for_region(region, bucket_pacific, bucket_non_pacific)
    owner = owner_for_region(region, owner_pacific, owner_non_pacific, product_owner)

    # TODO: Handle different bucket formats more robustly. For now we support:
    # "data.ldn.auspatious.com" to "https://data.ldn.auspatious.com"
    # "dep-public-staging" to "https://dep-public-staging.s3.us-west-2.amazonaws.com"
    if bucket.startswith("https://"):
        full_path_prefix = bucket
    elif "." in bucket:
        full_path_prefix = f"https://{bucket}"
    else:
        full_path_prefix = f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com"

    if decimated:
        typer.echo("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    # Configure for dask and reading data
    _ = configure_s3_access(requester_pays=True)
    # Configure for checking item existence
    client = boto3.client("s3")

    # Check if we've done this tile before
    itempath = S3ItemPath(
        prefix=owner,
        bucket=bucket,
        sensor=SENSOR,
        dataset_id=GEOMAD_DATASET_ID,
        version=version,
        time=year,
        full_path_prefix=full_path_prefix,
    )
    stac_document = itempath.stac_path(tile_index, absolute=True)
    stac_key = itempath.stac_path(tile_index, absolute=False)

    # If we don't want to overwrite, and the destination file already exists, skip it
    if not overwrite and object_exists(bucket, stac_key, client=client):
        typer.echo(f"Item already exists at {stac_document}, skipping.")
        return
    else:
        if not overwrite:
            typer.echo(f"Item does not exist at {stac_document}, processing tile.")

    load_kwargs = {}

    # Searcher finds STAC Items
    searcher = PystacSearcher(
        catalog=USGS_CATALOG,
        collections=[USGS_COLLECTION],
        datetime=search_year,
        **search_kwargs,
    )

    # Loader loads the data from STAC Items.
    loader = OdcLoader(
        bands=LANDSAT_BANDS
        if all_bands
        else [
            "red",
            "green",
            "blue",
            "qa_pixel",
            "qa_radsat",
        ],  # Exclude NIR and 2 SWIR bands.
        chunks={"x": xy_chunk_size, "y": xy_chunk_size, "time": 1},
        groupby="solar_day",
        fuse_func={
            "qa_pixel": "ldn.geomad.fuse_qa_pixel",  # This makes the qa_pixel data temporally merge correctly (grouped by solar day).
        },
        fail_on_error=False,  # We don't control the Landsat data so it may have issues, but we still want to load what we can.
        **load_kwargs,
    )

    # AWS Writer, to write results
    writer = AwsDsCogWriter(itempath, write_multithreaded=True)

    # Metadata creator
    stac_creator = StacCreator(
        collection_url_root=f"{full_path_prefix}/#{owner}_{SENSOR}_{GEOMAD_DATASET_ID}/",
        itempath=itempath,
        with_raster=True,
    )

    processor = GeoMADProcessor(
        geomad_options=dict(
            work_chunks=(100, 100),
            num_threads=geomad_threads,
            maxiters=100,
            scale=LANDSAT_SCALE,
            offset=LANDSAT_OFFSET,
            is_float=False,
        ),
        min_timesteps=3,
        drop_vars=["qa_pixel", "qa_radsat"],
        mask_clouds_kwargs={
            # Opening(3) removes isolated 1-3 pixel false cloud flags. These should not be dilated.
            # Dilation(3) grows remaining cloud masks by 3 pixels to catch haze/edges
            "filters": [("opening", 3), ("dilation", 5), ("erosion", 2)],
            "mask_shadow": mask_shadow,
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
                id=tile_index,  # TODO: Check this type
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
        sys.exit(EXIT_SKIP)

    except InsufficientScenesError as e:
        typer.echo(f"Failed to process with error: {e}")
        sys.exit(EXIT_SKIP)

    except KilledWorker as e:
        typer.echo(f"Failed to process with error: {e}")
        sys.exit(EXIT_OOM)

    except Exception as e:
        typer.echo(f"Failed to process with error: {e}")
        raise  # let it exit 1 naturally with full traceback

    typer.echo(f"Finished writing to {stac_document}")

    return
