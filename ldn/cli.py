import logging
import sys
import json

import boto3
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
import obstore
from rustac import write_sync

from dep_tools.exceptions import EmptyCollectionError
from dask.distributed import Client as DaskClient

from cogeo_mosaic.backends import MosaicBackend
from cogeo_mosaic.mosaic import MosaicJSON
from pystac import ItemCollection
from rustac import search_sync
from shapely.geometry import mapping, shape

from ldn.geomad import (
    GeoMADProcessor,
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
from ldn.cli_classify import classify_app
from ldn.grids import get_gridspec
from ldn.utils import GEOMAD_VERSION, LdnError, PREDICTION_VERSION

app = typer.Typer()
logger = logging.getLogger(__name__)

# All files will inherit this logging configuration so we only write once
# Set the default logging level to WARNING to avoid info logs from libraries
logging.basicConfig(
    level=logging.WARNING,  # Package logging level.
    format="%(asctime)s | %(levelname)s | %(module)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
    force=True,
)
logging.getLogger("ldn").setLevel(logging.INFO)  # Our logging level.

# Add the subcommands
app.add_typer(
    cli_grid_app, name="grid", help="Commands for working with the ODC Geo Grid."
)
app.add_typer(
    classify_app, name="classify", help="Commands for classifying/predicting LULC."
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

    if len(years_list) == 0:
        raise LdnError("Must provide at least one year.")
    if not all(y.isdigit() for y in years_list):
        raise LdnError("Years must be integers")

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
    logger.info(
        f"{len(tasks)} tasks written to tasks.json for years: {years} and grids: {grids}."
    )
    return


@app.command()
def geomad(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    region: Annotated[Literal["pacific", "non-pacific"], typer.Option()],
    product_owner: Annotated[str | None, typer.Option()] = None,
    bucket: Annotated[str, typer.Option()] = "data.ldn.auspatious.com",
    overwrite: Annotated[bool, typer.Option()] = False,
    decimated: Annotated[bool, typer.Option()] = False,
    include_shadow: Annotated[
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
        f"all_bands={all_bands} include_shadow={include_shadow} memory={memory_limit} workers={n_workers} threads={threads_per_worker} "
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

    # Fixed variables
    sensor = "ls"
    dataset_id = "geomad"

    # Set up variables and check
    tile_index = tuple(map(int, tile_id.split("_")))

    grid = get_gridspec(region=region)
    geobox = grid.tile_geobox(tile_index)

    if bucket.startswith("https://"):
        full_path_prefix = bucket
    else:
        full_path_prefix = f"https://{bucket}"

    if decimated:
        typer.echo("Warning, using decimated (low resolution) for testing purposes.")
        geobox = geobox.zoom_out(10)

    # Configure for dask and reading data
    _ = configure_s3_access(requester_pays=True)
    # Configure for checking item existence
    client = boto3.client("s3")

    if product_owner is not None:
        prefix = product_owner
    else:
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
        raise LdnError(f"Item already exists at {stac_document}")
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
        nodata=0,
        fail_on_error=False,  # We don't control the Landsat data so it may have issues, but we still want to load what we can.
        **load_kwargs,
    )

    # AWS Writer, to write results
    writer = AwsDsCogWriter(itempath, write_multithreaded=True)

    # Metadata creator
    stac_creator = StacCreator(
        collection_url_root=f"{full_path_prefix}/#{prefix}_{sensor}_{dataset_id}/",
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
            nodata=0,
            is_float=False,
        ),
        min_timesteps=10,
        drop_vars=["qa_pixel", "qa_radsat"],
        mask_clouds_kwargs={
            # Opening(3) removes isolated 1-3 pixel false cloud flags. These should not be dilated.
            # Dilation(3) grows remaining cloud masks by 3 pixels to catch haze/edges
            "filters": [("opening", 3), ("dilation", 5), ("erosion", 2)],
            "include_shadow": include_shadow,
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
        raise LdnError("No items found for this tile")
    except Exception as e:
        typer.echo(f"Failed to process with error: {e}")
        raise LdnError("Failed to process tile") from e

    typer.echo(f"Finished writing to {stac_document}")

    return


def _find_stac_items_s3(
    bucket: str,
    prefix: str,
    aws_region: str,
    suffix: str = ".stac-item.json",
    chunk_size: int = 200,
) -> list[str]:
    """List S3 keys ending in suffix under bucket/prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to search under.
        aws_region: AWS region of the bucket.
        suffix: File suffix to match.
        chunk_size: Number of objects per listing page.

    Returns:
        List of S3 keys (without the s3://bucket/ prefix) that match.
    """
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    matches: list[str] = []
    stream = obstore.list(store, prefix=prefix.lstrip("/"), chunk_size=chunk_size)

    for chunk in stream:
        for obj in chunk:
            path = obj.get("path", "")
            if path.endswith(suffix):
                matches.append(path)

    return matches


# TODO: Add chunking/streaming to prevent loading too much data into memory at once.
def _load_stac_docs(
    bucket: str,
    keys: list[str],
    aws_region: str,
) -> list[dict]:
    """Load STAC item JSON documents from S3 into memory.

    Args:
        bucket: S3 bucket name.
        keys: S3 object keys to load.
        aws_region: AWS region of the bucket.

    Returns:
        List of parsed STAC item dictionaries.
    """
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    docs: list[dict] = []

    for key in keys:
        raw = obstore.get(store, key)
        payload = raw.bytes()
        if hasattr(payload, "to_bytes"):
            payload = payload.to_bytes()
        elif not isinstance(payload, (bytes, bytearray)):
            payload = bytes(payload)
        docs.append(json.loads(payload.decode("utf-8")))

    return docs


@app.command("index-to-stac-geoparquet")
def _index_to_stac_geoparquet(
    prefix: str = typer.Option(
        ...,
        help="S3 path prefix to search for STAC items to index (e.g. for a given dataset).",
    ),
    output_filename: str = typer.Option(
        ..., help="Output filename for the STAC-Geoparquet index."
    ),
    version: str = typer.Option(..., help="Dataset version string e.g. '0-0-1'."),
    bucket: str = typer.Option(
        "data.ldn.auspatious.com", help="S3 bucket containing STAC items."
    ),
    aws_region: str = typer.Option("us-west-2", help="AWS region of the bucket."),
) -> None:
    """Build a STAC-Geoparquet index from all STAC items under a given S3 prefix and version."""
    prefix = f"{prefix}/{version}"
    parquet_key = f"{prefix}/{output_filename}.parquet"

    logger.info(f"Listing STAC items under s3://{bucket}/{prefix}")
    keys = _find_stac_items_s3(bucket, prefix, aws_region)
    logger.info(f"Found {len(keys)} STAC items")

    if len(keys) == 0:
        logger.warning("No STAC items found, nothing to index.")
        raise LdnError("No STAC items found, nothing to index.")

    logger.info("Loading STAC item documents into memory")
    docs = _load_stac_docs(bucket, keys, aws_region)
    logger.info(f"Loaded {len(docs)} STAC documents")

    logger.info(f"Writing STAC-Geoparquet to s3://{bucket}/{parquet_key}")
    store = obstore.store.S3Store(bucket=bucket, region=aws_region)
    write_sync(parquet_key, docs, store=store)

    logger.info(f"Wrote index with {len(docs)} items to s3://{bucket}/{parquet_key}")


def _stac_self_link(feature: dict) -> str:
    """Extract the STAC item self-link URL."""
    links = {link["rel"]: link["href"] for link in feature.get("links", [])}
    self_link = links.get("self")
    if self_link is None:
        raise LdnError(
            f"Feature {feature.get('id', 'unknown')} has no self link, cannot determine STAC item URL."
        )
    return self_link


def _build_mosaic_for_year(year: str, stac_geoparquet_url: str) -> MosaicJSON:
    """Read STAC-Geoparquet, filter by year, build mosaic.json."""

    logger.info(f"Building mosaic for year {year}")
    search_year = year
    int_year = int(year)
    # If we're in the LS7 era, use a buffered window of data
    if int_year <= 2012:
        logging.info(
            "GeoMAD used a year of data on either side to create the annual product so we need to search for +- 1 year."
        )
        search_year = f"{int_year - 1}/{int_year + 1}"

    item_collection = search_sync(stac_geoparquet_url, datetime=search_year)

    items = ItemCollection(item_collection)
    features = [f.to_dict() for f in items]

    if not features:
        raise LdnError(f"No STAC items found for year {year}")

    logger.info(f"  {len(features)} features found")

    # cogeo-mosaic requires Polygon geometries
    for feat in features:
        geom = shape(feat["geometry"])
        if geom.geom_type != "Polygon":
            geom = geom.convex_hull
        feat["geometry"] = mapping(geom)

    mosaic = MosaicJSON.from_features(
        features,
        minzoom=5,
        maxzoom=14,
        accessor=_stac_self_link,
    )

    return mosaic


# TODO: Make bucket and prefix variables.
@app.command()
def make_mosaics(
    years: Annotated[
        str,
        typer.Option(
            help="Either a comma-separated list of years (e.g. '2020,2021') or a range of years (e.g. '2020-2025') to build mosaics for."
        ),
    ],
    dataset: Annotated[
        Literal["all", "geomad", "prediction"],
        typer.Option(
            help="Which dataset to build mosaics for, either 'all', 'geomad' or 'prediction'."
        ),
    ],
    version_geomad: Annotated[
        str,
        typer.Option(
            help=f"Version string to use for the GeoMAD mosaic files, e.g. '{GEOMAD_VERSION}'."
        ),
    ] = GEOMAD_VERSION,
    version_prediction: Annotated[
        str,
        typer.Option(
            help=f"Version string to use for the Prediction mosaic files, e.g. '{PREDICTION_VERSION}'."
        ),
    ] = PREDICTION_VERSION,
) -> None:
    """Make mosaic.jsons per year for GeoMedian and Prediction results from their respective STAC-Geoparquet files."""

    logger.info(f"Making mosaics for dataset '{dataset}' and years: {years}")
    if "-" in years:
        start_year, end_year = map(int, years.split("-"))
        years_list = [str(y) for y in range(start_year, end_year + 1)]
    else:
        years_list = [y.strip() for y in years.split(",")]

    if any(int(y) < 2000 for y in years_list) or any(int(y) > 2025 for y in years_list):
        raise LdnError("Years must be between 2000 and 2025 inclusive.")

    # MosaicBackend needs s3:// style paths.
    output_path_geomad = (
        f"s3://data.ldn.auspatious.com/ausp_ls_geomad/{version_geomad}/mosaics/"
    )
    output_path_prediction = f"s3://data.ldn.auspatious.com/ausp_ls_lulc_prediction/{version_prediction}/mosaics/"

    datasets = []
    if dataset in ["prediction", "all"]:
        datasets.append(
            (
                "prediction",
                f"https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_lulc_prediction/{version_prediction}/ausp_ls_lulc_prediction.parquet",
                output_path_prediction,
            )
        )
    if dataset in ["geomad", "all"]:
        datasets.append(
            (
                "geomad",
                f"https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/ausp_ls_geomad/{version_geomad}/ausp_ls_geomad.parquet",
                output_path_geomad,
            )
        )

    # Build mosaics for all years in the dataset
    for dataset_name, stac_geoparquet_url, output_path in datasets:
        logger.info(f"Building mosaics for '{dataset_name}' dataset.")
        for _year in years_list:
            mosaic = _build_mosaic_for_year(_year, stac_geoparquet_url)
            logger.info(f"  {_year} built successfully.")
            # Write to S3.
            out_path = f"{output_path}{dataset_name}_{_year}_mosaic.json"

            with MosaicBackend(out_path, mosaic_def=mosaic) as m:
                m.write(overwrite=True)

            logger.info(f"  {_year} written to {out_path}")

    logger.info("Finished writing mosaics.")
