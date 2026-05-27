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
from ldn.utils import (
    GEOMAD_VERSION,
    GEOMAD_DATASET_ID,
    PREDICTION_DATASET_ID,
    SENSOR,
    LdnError,
    PREDICTION_VERSION,
    PACIFIC_BUCKET,
    NON_PACIFIC_BUCKET,
    PACIFIC_OWNER,
    NON_PACIFIC_OWNER,
    bucket_for_region,
    owner_for_region,
    dataset_prefix,
)

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
    region: Annotated[Literal["all", "pacific", "non-pacific"], typer.Option()] = "all",
) -> None:
    """Print all tasks for given years for either all grids, or just the Pacific or non-Pacific grid."""
    logger.info(f"Generating tasks for years: {years} and region: {region}")

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

    tiles = get_grid_tiles(format="list", grids=region, overwrite=False)

    logger.info(
        f"Number of tasks: {len(years_list) * len(tiles)} (years: {len(years_list)}, tiles: {len(tiles)})"
    )

    tasks = []
    for year in years_list:
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
        f"{len(tasks)} tasks written to tasks.json for years: {years} and region: {region}."
    )
    return


# This command is helpful for developing.
# It is basically a performance optimization to prevent a lot of pods spinning up to discover that their output exists and shouldn't be overwritten.
# It duplicates a lot of code and isn't very clean. If something was changed in geomad, this would be out of sync and cause issues.
@app.command()
def filter_tasks(
    tasks_json: Annotated[str, typer.Option(help="JSON string of tasks to filter.")],
    version: Annotated[
        str,
        typer.Option(
            help="Version string for the data product. Depending on dataset parameter."
        ),
    ],
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
    dataset: Annotated[
        Literal["geomad", "prediction"], typer.Option(help="Dataset name.")
    ] = "geomad",
    overwrite: Annotated[
        bool, typer.Option(help="If true, skip filtering and pass all tasks through.")
    ] = False,
) -> None:
    """Filter tasks by checking if the output STAC item already exists in S3.

    Takes a JSON array of tasks (with id, year, region fields) and outputs
    only those tasks whose output STAC items do not yet exist.
    """
    tasks = json.loads(tasks_json)

    if overwrite:
        logger.info(f"Overwrite enabled, passing all {len(tasks)} tasks through.")
        typer.echo(json.dumps(tasks))
        return

    logger.info(f"Filtering {len(tasks)} tasks for existing outputs.")

    # Map CLI dataset name to S3 dataset_id
    dataset_id = PREDICTION_DATASET_ID if dataset == "prediction" else GEOMAD_DATASET_ID

    # Normalize version the same way S3ItemPath does internally
    version = version.replace(".", "-")

    client = boto3.client("s3")

    def _full_path_prefix_for_bucket(bucket: str) -> str:
        """Return the full path prefix URL for a bucket."""
        if bucket.startswith("https://"):
            return bucket
        elif "." in bucket:
            return f"https://{bucket}"
        else:
            return f"https://{bucket}.s3.us-west-2.amazonaws.com"

    # Collect unique (bucket, owner) combos to list
    region_combos: set[tuple[str, str]] = set()
    for task in tasks:
        r = task["region"]
        region_combos.add(
            (
                bucket_for_region(r, bucket_pacific, bucket_non_pacific),
                owner_for_region(r, owner_pacific, owner_non_pacific),
            )
        )

    # List all STAC items under each prefix in one paginated call
    existing_keys: dict[str, set[str]] = {}
    for combo_bucket, combo_owner in region_combos:
        full_prefix = dataset_prefix(combo_owner, dataset_id)
        s3_prefix = f"{full_prefix}/{version}/"
        paginator = client.get_paginator("list_objects_v2")
        key_set: set[str] = set()
        for page in paginator.paginate(Bucket=combo_bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".stac-item.json"):
                    key_set.add(key)
        existing_keys[f"{combo_bucket}/{combo_owner}"] = key_set

    total_existing = sum(len(v) for v in existing_keys.values())
    logger.info(f"Found {total_existing} existing STAC items in S3.")

    # Check each task against the set
    remaining = []
    for task in tasks:
        tile_index = tuple(map(int, task["id"].split("_")))
        r = task["region"]
        bucket = bucket_for_region(r, bucket_pacific, bucket_non_pacific)
        owner = owner_for_region(r, owner_pacific, owner_non_pacific)
        full_path_prefix = _full_path_prefix_for_bucket(bucket)

        itempath = S3ItemPath(
            prefix=owner,
            bucket=bucket,
            sensor=SENSOR,
            dataset_id=dataset_id,
            version=version,
            time=task["year"],
            full_path_prefix=full_path_prefix,
        )
        stac_key = itempath.stac_path(tile_index, absolute=False)

        lookup_key = f"{bucket}/{owner}"
        if stac_key not in existing_keys.get(lookup_key, set()):
            remaining.append(task)

    logger.info(
        f"Filtered: {len(tasks) - len(remaining)} already exist, {len(remaining)} remaining."
    )
    typer.echo(json.dumps(remaining))


@app.command()
def geomad(
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
    owner = owner_for_region(region, owner_pacific, owner_non_pacific)
    if product_owner is not None:
        owner = product_owner

    # TODO: Handle different bucket formats more robustly. For now we support:
    # "data.ldn.auspatious.com" to "https://data.ldn.auspatious.com"
    # "dep-public-staging" to "https://dep-public-staging.s3.us-west-2.amazonaws.com"
    if bucket.startswith("https://"):
        full_path_prefix = bucket
    elif "." in bucket:
        full_path_prefix = f"https://{bucket}"
    else:
        full_path_prefix = f"https://{bucket}.s3.us-west-2.amazonaws.com"  # TODO: can the region be dynamic?

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
    dataset: Literal["all", "geomad", "prediction"] = typer.Option(
        ..., help="Dataset type to index: 'all', 'geomad', or 'prediction'."
    ),
    region: Literal["all", "pacific", "non-pacific"] = typer.Option(
        ..., help="Region to index: 'all', 'pacific', or 'non-pacific'."
    ),
    version_geomad: str = typer.Option(
        GEOMAD_VERSION, help="Version string for GeoMAD dataset."
    ),
    version_prediction: str = typer.Option(
        PREDICTION_VERSION, help="Version string for prediction dataset."
    ),
    bucket_pacific: str = typer.Option(
        PACIFIC_BUCKET, help="S3 bucket for pacific data."
    ),
    bucket_non_pacific: str = typer.Option(
        NON_PACIFIC_BUCKET, help="S3 bucket for non-pacific data."
    ),
    owner_pacific: str = typer.Option(
        PACIFIC_OWNER, help="Short owner prefix for pacific (e.g. 'dep')."
    ),
    owner_non_pacific: str = typer.Option(
        NON_PACIFIC_OWNER, help="Short owner prefix for non-pacific (e.g. 'ci')."
    ),
    aws_region: str = typer.Option("us-west-2", help="AWS region of the buckets."),
) -> None:
    """Build STAC-Geoparquet indexes from STAC items for given dataset(s) and region(s)."""
    targets: list[tuple[str, str, str]] = []

    regions = ["pacific", "non-pacific"] if region == "all" else [region]
    datasets = ["geomad", "prediction"] if dataset == "all" else [dataset]

    for r in regions:
        bucket = bucket_for_region(r, bucket_pacific, bucket_non_pacific)
        owner = owner_for_region(r, owner_pacific, owner_non_pacific)
        for d in datasets:
            if d == "geomad":
                prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
                version = version_geomad
            else:
                prefix = dataset_prefix(owner, PREDICTION_DATASET_ID)
                version = version_prediction
            targets.append((bucket, prefix, version))

    for target_bucket, target_prefix, target_version in targets:
        _run_index(
            target_bucket, target_prefix, target_prefix, target_version, aws_region
        )


def _run_index(
    bucket: str, prefix: str, output_filename: str, version: str, aws_region: str
) -> None:
    """Run the STAC-Geoparquet indexing for a single bucket/prefix."""
    full_prefix = f"{prefix}/{version}"
    parquet_key = f"{full_prefix}/{output_filename}.parquet"

    logger.info(f"Listing STAC items under s3://{bucket}/{full_prefix}")
    keys = _find_stac_items_s3(bucket, full_prefix, aws_region)
    logger.info(f"Found {len(keys)} STAC items")

    if len(keys) == 0:
        logger.warning(
            f"No STAC items found under s3://{bucket}/{full_prefix}, skipping."
        )
        return

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


def _build_mosaic_for_year(year: str, features: list[dict]) -> MosaicJSON:
    """Filter features by year and build a MosaicJSON.

    Args:
        year: Year string to filter for.
        features: All STAC item feature dicts from the index.

    Returns:
        MosaicJSON for the matching features.
    """
    int_year = int(year)

    def _matches_year(feat: dict) -> bool:
        """Check if a feature's datetime falls within the target year."""
        dt_str = feat.get("properties", {}).get("datetime", "")
        if not dt_str:
            return False
        feat_year = int(dt_str[:4])
        if int_year <= 2012:
            return abs(feat_year - int_year) <= 1
        return feat_year == int_year

    year_features = [f for f in features if _matches_year(f)]

    if not year_features:
        raise LdnError(f"No STAC items found for year {year}")

    logger.info(f"  {year}: {len(year_features)} features")

    mosaic = MosaicJSON.from_features(
        year_features,
        minzoom=5,
        maxzoom=14,
        accessor=_stac_self_link,
    )

    return mosaic


def _load_all_features(stac_geoparquet_url: str) -> list[dict]:
    """Load all STAC items from a geoparquet and prepare geometries for mosaic building.

    Args:
        stac_geoparquet_url: URL to the STAC-Geoparquet file.

    Returns:
        List of feature dicts with Polygon geometries.
    """
    item_collection = search_sync(stac_geoparquet_url)
    items = ItemCollection(item_collection)
    features = [f.to_dict() for f in items]

    for feat in features:
        geom = shape(feat["geometry"])
        if geom.geom_type != "Polygon":
            geom = geom.convex_hull
        feat["geometry"] = mapping(geom)

    return features


def _extract_years(features: list[dict]) -> list[str]:
    """Extract sorted unique years from STAC feature datetimes.

    Args:
        features: List of STAC item feature dicts.

    Returns:
        Sorted list of year strings.
    """
    years: set[str] = set()
    for feat in features:
        dt_str = feat.get("properties", {}).get("datetime", "")
        if dt_str:
            years.add(dt_str[:4])
    return sorted(years)


@app.command()
def make_mosaics(
    dataset: Annotated[
        Literal["all", "geomad", "prediction"],
        typer.Option(
            help="Which dataset to build mosaics for: 'all', 'geomad', or 'prediction'."
        ),
    ],
    region: Annotated[
        Literal["all", "pacific", "non-pacific"],
        typer.Option(
            help="Region to build mosaics for. 'all' builds both pacific and non-pacific."
        ),
    ] = "all",
    version_geomad: Annotated[
        str,
        typer.Option(help="Version string for GeoMAD dataset."),
    ] = GEOMAD_VERSION,
    version_prediction: Annotated[
        str,
        typer.Option(help="Version string for prediction dataset."),
    ] = PREDICTION_VERSION,
    bucket_pacific: Annotated[
        str,
        typer.Option(help="S3 bucket for pacific data."),
    ] = PACIFIC_BUCKET,
    bucket_non_pacific: Annotated[
        str,
        typer.Option(help="S3 bucket for non-pacific data."),
    ] = NON_PACIFIC_BUCKET,
    owner_pacific: Annotated[
        str,
        typer.Option(help="Short owner prefix for pacific (e.g. 'dep')."),
    ] = PACIFIC_OWNER,
    owner_non_pacific: Annotated[
        str,
        typer.Option(help="Short owner prefix for non-pacific (e.g. 'ci')."),
    ] = NON_PACIFIC_OWNER,
) -> None:
    """Make mosaic.jsons per year for GeoMedian and Prediction results from their respective STAC-Geoparquet files.

    Years are auto-detected from the STAC-Geoparquet index.
    """
    logger.info(f"Making mosaics for dataset '{dataset}', region '{region}'")

    regions = ["pacific", "non-pacific"] if region == "all" else [region]
    datasets_list = ["geomad", "prediction"] if dataset == "all" else [dataset]

    mosaic_targets: list[tuple[str, str, str, str]] = []
    for r in regions:
        bucket = bucket_for_region(r, bucket_pacific, bucket_non_pacific)
        owner = owner_for_region(r, owner_pacific, owner_non_pacific)
        if "." in bucket:
            base_url = f"https://{bucket}"
        else:
            base_url = f"https://s3.us-west-2.amazonaws.com/{bucket}"
        for d in datasets_list:
            if d == "geomad":
                prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
                ver = version_geomad
            else:
                prefix = dataset_prefix(owner, PREDICTION_DATASET_ID)
                ver = version_prediction
            output_path = f"s3://{bucket}/{prefix}/{ver}/mosaics/"
            parquet_url = f"{base_url}/{prefix}/{ver}/{prefix}.parquet"
            mosaic_targets.append((f"{d} ({r})", d, parquet_url, output_path))

    for display_name, dataset_name, stac_geoparquet_url, output_path in mosaic_targets:
        logger.info(f"Loading index for '{display_name}' from {stac_geoparquet_url}")
        features = _load_all_features(stac_geoparquet_url)

        if not features:
            logger.warning(f"No features found for '{display_name}', skipping.")
            continue

        years_list = _extract_years(features)
        logger.info(
            f"Found {len(features)} features across {len(years_list)} years: {years_list[0]}-{years_list[-1]}"
        )

        for _year in years_list:
            mosaic = _build_mosaic_for_year(_year, features)
            logger.info(f"  {_year} built successfully.")
            out_path = f"{output_path}{dataset_name}_{_year}_mosaic.json"

            with MosaicBackend(out_path, mosaic_def=mosaic) as m:
                m.write(overwrite=True)

            logger.info(f"  {_year} written to {out_path}")

    logger.info("Finished writing mosaics.")
