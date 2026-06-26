import logging
import sys
from typing import Literal

import typer
from dask.distributed import Client as DaskClient
from dask.distributed import KilledWorker
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoader
from dep_tools.searchers import PystacSearcher
from typing_extensions import Annotated

from ldn.aws import configure_s3_access_profile, s3_client
from ldn.geomad import (
    LANDSAT_BANDS,
    LANDSAT_OFFSET,
    LANDSAT_SCALE,
    USGS_CATALOG,
    USGS_COLLECTION,
    GeoMADProcessor,
    InsufficientScenesError,
)
from ldn.geomad import (
    AwsStacTask as Task,
)
from ldn.grids import get_gridspec
from ldn.raster import build_pipeline_components, get_collection_url_root
from ldn.utils import (
    GEOMAD_DATASET_ID,
    GEOMAD_VERSION,
    LS7_YEAR_THRESHOLD,
    SENSOR,
    SOURCE_COOP_PREFIX_GEOMAD,
    get_env_var,
    is_bucket_source_coop,
    owner_for_region,
    parse_tile_id,
)

geomad_app = typer.Typer()
logger = logging.getLogger(__name__)

EXIT_OOM = 42  # KilledWorker — retry with more resources
EXIT_SKIP = 43  # too few scenes / no items — expected, don't retry


def _count_scenes(
    tile_id_tuple: tuple[int, int] = typer.Option(..., help="Tile ID tuple to count scenes for."),
    region: Literal["pacific", "non-pacific"] = typer.Option(..., help="Region tile is in."),
    year: str = typer.Option(..., help="Year to count scenes for."),
    include_t2: bool = typer.Option(
        False,
        help="If True, include tier 2 scenes in the count. Defaults to False (just tier 1).",
    ),
) -> int:
    """Count (tier 1 or all) scenes per tile and year."""
    grid = get_gridspec(region=region)
    geobox = grid.tile_geobox(tile_id_tuple)

    query = {} if include_t2 else {"landsat:collection_category": {"in": ["T1"]}}

    searcher = PystacSearcher(
        catalog=USGS_CATALOG,
        collections=[USGS_COLLECTION],
        datetime=year,
        query=query,
        raise_empty_collection_error=False,  # Don't raise an error if no scenes found, just return count of 0
    )

    items = searcher.search(geobox)

    dates = {item.datetime.date() for item in items}
    count = len(dates)

    log_t2 = "(T1 and T2)" if include_t2 else "(T1 only)"
    logger.info(f"Found {count} {log_t2} scenes for this tile/year (grouped by day)")

    return count


@geomad_app.command()
def run(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    region: Annotated[Literal["pacific", "non-pacific"], typer.Option()],
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: Annotated[str | None, typer.Option(help="Override the region-derived owner prefix.")] = None,
    overwrite: Annotated[bool, typer.Option()] = False,
    decimated: Annotated[bool, typer.Option()] = False,
    integration_test: Annotated[bool, typer.Option()] = False,
    mask_shadow: Annotated[
        bool,
        typer.Option(help="True to mask cloud shadows, false to not mask them (leave them in). Defaults to True."),
    ] = True,
    ls7_buffer_years: Annotated[
        int,
        typer.Option(help=f"Temporal buffer for LS7 era (<={LS7_YEAR_THRESHOLD}). E.g. 1 searches year-1 to year+1."),
    ] = 1,
    all_bands: Annotated[bool, typer.Option()] = True,
    memory_limit: Annotated[str, typer.Option()] = "10GB",
    n_workers: Annotated[int, typer.Option()] = 2,
    threads_per_worker: Annotated[int, typer.Option()] = 16,
    xy_chunk_size: Annotated[int, typer.Option()] = 2048,
    geomad_threads: Annotated[int, typer.Option()] = 10,
    collection_url_root: Annotated[str | None, typer.Option(help="Override the default collection URL root.")] = None,
    sensor: Annotated[str, typer.Option(help="Sensor name, e.g. 'ls'.")] = SENSOR,
) -> None:
    """Run GeoMAD processing on a single tile for a year.

    Searches USGS STAC for Landsat scenes covering the given tile and year,
    applies cloud masking, computes the geometric median and median absolute
    deviations (GeoMAD), and writes COG outputs to S3.

    For years in the Landsat 7 era (<={LS7_YEAR_THRESHOLD}), a buffered temporal window
    controlled by --ls7-buffer-years is used to gather enough clear
    observations. Pacific tiles may additionally include Tier 2 data.
    """
    bucket = bucket or get_env_var("BUCKET")  # Default

    logger.info(
        f"tile={tile_id} year={year} version={version} region={region} bucket={bucket} overwrite={overwrite} "
        f"decimated={decimated} all_bands={all_bands} mask_shadow={mask_shadow} geomad_threads={geomad_threads}",
    )
    logger.info(
        f"Dask config: n_workers={n_workers}, threads_per_worker={threads_per_worker}, "
        f"memory_limit={memory_limit}, xy_chunk_size={xy_chunk_size}"
    )

    if version != GEOMAD_VERSION:
        logger.info(f"Overriding the latest GeoMAD version ({GEOMAD_VERSION}) with the specified version ({version}).")

    tile_id_tuple = parse_tile_id(tile_id)

    year_int = int(year)
    search_year = year
    search_kwargs = {"query": {"landsat:collection_category": {"in": ["T1"]}}}

    min_scenes_threshold = 20

    scene_count_without_t2 = _count_scenes(tile_id_tuple=tile_id_tuple, year=year, region=region, include_t2=False)
    logger.info(f"Scene count (tier 1) for tile/year: {scene_count_without_t2}")

    if scene_count_without_t2 >= min_scenes_threshold:
        logger.info(f"Scene count ({scene_count_without_t2}) is sufficient with T1 only.")
        # search_kwargs already set to T1 only, search_year already set to year

    else:
        logger.info(f"Scene count ({scene_count_without_t2}) is below {min_scenes_threshold}, trying with T2 too.")
        scene_count_with_t2 = _count_scenes(tile_id_tuple=tile_id_tuple, year=year, region=region, include_t2=True)

        if scene_count_with_t2 >= min_scenes_threshold:
            logger.info(f"Scene count with T2 ({scene_count_with_t2}) is sufficient, using T1 and T2.")
            search_kwargs = {}  # Include T2

        else:
            logger.info(
                f"Scene count with T2 ({scene_count_with_t2}) is still below {min_scenes_threshold}. "
                f"Adding LS7 buffered temporal search as well as T1 and T2 data."
            )
            search_kwargs = {}  # Include T2

            if year_int <= LS7_YEAR_THRESHOLD:
                year_start = year_int - ls7_buffer_years
                year_end = year_int + ls7_buffer_years
                search_year = f"{year_start}/{year_end}"
                logger.info(f"Using {ls7_buffer_years}-year buffered temporal search for LS7 era: {search_year}")
            else:
                logger.info("Not in LS7 era so not using buffered temporal search.")

    grid = get_gridspec(region=region)
    geobox = grid.tile_geobox(tile_id_tuple)

    owner = owner_for_region(region, product_owner)

    if decimated:
        logger.warning("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    if integration_test:
        logger.warning(
            "Integration test mode: using 5x5 pixel geobox and limiting to 3 items for very fast processing."
        )
        geobox = geobox[0:5, 0:5]
        search_kwargs["max_items"] = 4
        n_workers = 1
        threads_per_worker = 1
        memory_limit = "1GB"
        # TODO: Implement these to further speed up.
        # mask_clouds_kwargs["filters"] = None
        # geomad_options["maxiters"] = 1

    configure_s3_access_profile()  # Access must be configured here for Dask.

    collection_url_root = collection_url_root or get_collection_url_root(bucket, owner, sensor, GEOMAD_DATASET_ID)

    components = build_pipeline_components(
        tile_id_tuple,
        year,
        version,
        bucket,
        owner,
        GEOMAD_DATASET_ID,
        SOURCE_COOP_PREFIX_GEOMAD if is_bucket_source_coop(bucket) else None,
        overwrite,
        collection_url_root=collection_url_root,
        s3_client=s3_client,
    )
    if components is None:
        return  # Task exists and overwrite is False, so skipping processing.
    itempath, stac_creator, writer = components

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
            # This makes the qa_pixel data temporally merge correctly (grouped by solar day).
            "qa_pixel": "ldn.geomad.fuse_qa_pixel",
        },
        fail_on_error=False,  # We don't control the Landsat data so it may have issues. We load what we can.
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
                id=tile_id_tuple,
                area=geobox,
                searcher=searcher,
                loader=loader,
                processor=processor,
                logger=logger,
                writer=writer,
                stac_creator=stac_creator,
            ).run()
            logger.info(
                f"Completed processing. Wrote {len(paths)} files to {itempath.stac_path(tile_id_tuple, absolute=True)}"
            )

    except EmptyCollectionError:
        logger.exception("No items found for this tile")
        sys.exit(EXIT_SKIP)

    except InsufficientScenesError as e:
        logger.exception(f"Failed to process with error: {e}")
        sys.exit(EXIT_SKIP)

    except KilledWorker as e:
        logger.exception(f"Failed to process with error: {e}")
        sys.exit(EXIT_OOM)

    except Exception:
        logger.exception("Failed to process with error")
        raise  # let it exit 1 naturally with full traceback
