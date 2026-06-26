import asyncio
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

import rustac
import typer
from cogeo_mosaic.mosaic import MosaicJSON
from dotenv import load_dotenv
from pystac import Item, ItemCollection
from rustac import write_sync
from shapely.geometry import mapping, shape
from typing_extensions import Annotated

load_dotenv()  # Load AWS credentials from .env file (for local dev). Do this before imports.

from ldn import get_version
from ldn.aws import s3_client, credential_provider
from ldn.cli_collection import collection_app
from ldn.cli_geomad import geomad_app
from ldn.cli_grid import cli_grid_app
from ldn.cli_lulc import classify_app
from ldn.grids import get_grid_tiles
from ldn.raster import PrefixedS3ItemPath
from ldn.training_data import cli_training_app
from ldn.utils import (
    GEOMAD_VERSION,
    LULC_VERSION,
    SENSOR,
    LdnError,
    build_prefix,
    dataset_prefix,
    get_env_var,
    get_full_path_prefix,
    get_stac_geoparquet_key,
    get_stac_geoparquet_url,
    is_bucket_source_coop,
    load_stac_geoparquet_features,
    owner_for_region,
    parse_tile_id,
    parse_years,
    source_coop_prefix,
    version_for_dataset,
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
app.add_typer(cli_grid_app, name="grid", help="Commands for working with the ODC Geo Grid.")
app.add_typer(classify_app, name="lulc", help="Commands for LULC classification.")
app.add_typer(cli_training_app, name="training", help="Commands for generating training data.")
app.add_typer(geomad_app, name="geomad", help="Commands for working with GeoMAD.")
app.add_typer(collection_app, name="collection", help="Commands for working with STAC Collections and indexes.")


# Work for version and --version
@app.command()
def version() -> None:
    """Echo the version of the software."""

    version = get_version()
    logger.info(version)

    return


if __name__ == "__main__":
    app()


def _find_stac_items_s3(
    bucket: str,
    prefix: str,
) -> list[str]:
    """List S3 keys ending in suffix under bucket/prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to search under.

    Returns:
        List of S3 keys (no bucket or prefix) that match.
    """
    suffix = ".stac-item.json"
    chunk_size = 200
    paginator = s3_client.get_paginator("list_objects_v2")

    matches: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": chunk_size}):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                matches.append(key)

    return matches


# _find_existing_tasks should be updated to use the same read logic for all buckets (with auth).
def _find_existing_tasks(
    input_tasks: list[dict],
    version: str,
    dataset_id: Literal["geomad", "lulc"],
    bucket: str,
    product_owner: str | None,
    sensor: str,
):
    """Check which tasks already have outputs using S3 listing.

    Lists all STAC items under each (bucket, owner) prefix and returns
    a set of (id, year) tuples for tasks whose output already exists.
    """
    region_owners: set[str] = set()
    if product_owner:
        logger.info(f"Using product owner override: {product_owner}")
        region_owners.add(product_owner)
    else:
        logger.info("No product owner override, deriving owner from input task region/s.")
        for task in input_tasks:
            r = task["region"]
            region_owners.add(owner_for_region(r, product_owner))

    _is_bucket_source_coop = is_bucket_source_coop(bucket)
    sc_prefix = source_coop_prefix(dataset_id) if _is_bucket_source_coop else None

    # List all STAC items under each region prefix
    existing_keys: dict[str, set[str]] = {}
    for region_owner in region_owners:
        # TODO: Use build_prefix() here.
        s3_prefix = f"{dataset_prefix(region_owner, sensor, dataset_id)}/{version}/"
        if _is_bucket_source_coop and sc_prefix:
            s3_prefix = f"{sc_prefix}/{s3_prefix}"
        logger.info(f"For region {region_owner}, finding STAC items under prefix: {s3_prefix}")
        keys = _find_stac_items_s3(bucket, s3_prefix)
        existing_keys[f"{bucket}/{region_owner}"] = set(keys)

    total_existing = sum(len(v) for v in existing_keys.values())
    logger.info(
        f"Found {total_existing} existing STAC items in S3 (this value may be more than how many are going to be "
        f"processed because of input parameters to print-tasks)."
    )

    # Check each task against the set
    existing_tasks: set[tuple[str, str]] = set()
    for task in input_tasks:
        tile_id_tuple = parse_tile_id(task["id"])
        r = task["region"]
        owner = owner_for_region(r, product_owner)
        full_path_prefix = get_full_path_prefix(bucket)

        itempath = PrefixedS3ItemPath(
            key_prefix=sc_prefix if _is_bucket_source_coop else None,
            prefix=owner,
            bucket=bucket,
            sensor=SENSOR,
            dataset_id=dataset_id,
            version=version,
            time=task["year"],
            full_path_prefix=full_path_prefix,
        )
        stac_key = itempath.stac_path(tile_id_tuple, absolute=False)

        lookup_key = f"{bucket}/{owner}"
        if stac_key in existing_keys.get(lookup_key, set()):
            existing_tasks.add((task["id"], task["year"]))

    return existing_tasks


@app.command()
def print_tasks(
    years: Annotated[str, typer.Option()],
    region: Annotated[Literal["all", "pacific", "non-pacific"], typer.Option()] = "all",
    dataset: Annotated[Literal["geomad", "lulc"], typer.Option(help="Dataset name.")] = "geomad",
    geomad_version: Annotated[str, typer.Option(help="Version string for GeoMAD dataset.")] = GEOMAD_VERSION,
    lulc_version: Annotated[str, typer.Option(help="Version string for LULC dataset.")] = LULC_VERSION,
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: Annotated[str | None, typer.Option(help="Override the region-derived owner prefix.")] = None,
    overwrite: Annotated[bool, typer.Option(help="If true, skip filtering existing outputs.")] = False,
    sensor: Annotated[str, typer.Option(help="Sensor name, e.g. 'ls' for Landsat.")] = SENSOR,
) -> None:
    """Print tasks for given years, optionally filtering out those with existing outputs."""
    logger.info(f"Generating tasks for years: {years} and region: {region}")
    bucket = bucket or get_env_var("BUCKET")  # Default
    years_list = parse_years(years)
    tiles = get_grid_tiles(format="list", grids=region, overwrite=False)
    logger.info(f"Number of tasks: {len(years_list) * len(tiles)} (years: {len(years_list)}, tiles: {len(tiles)})")

    input_tasks = []
    for year in years_list:
        for tile in tiles:
            input_tasks.append(
                {
                    "id": "_".join(str(i) for i in tile[0]),
                    "year": year,
                    "region": tile[1],
                }
            )

    # Filter out tasks whose output already exists in S3
    if not overwrite:
        version = version_for_dataset(dataset, geomad_version, lulc_version)
        existing = _find_existing_tasks(
            input_tasks,
            version,
            dataset,
            bucket,
            product_owner,
            sensor,
        )
        before_count = len(input_tasks)
        input_tasks = [t for t in input_tasks if (t["id"], t["year"]) not in existing]
        logger.info(f"Filtered: {before_count - len(input_tasks)} already exist, {len(input_tasks)} remaining.")
    else:
        logger.info("Overwrite enabled, skipping existence check.")

    tasks_json_str = json.dumps(input_tasks, separators=(",", ":"))
    sys.stdout.write(tasks_json_str)
    logger.info(f"{len(input_tasks)} tasks output for years: {years} and region: {region}.")
    return


async def _load_stac_docs_async(
    bucket: str,
    keys: list[str],
) -> list[dict]:
    """Load STAC item JSON documents from S3 concurrently.

    Args:
        bucket: S3 bucket name.
        keys: S3 object keys to load.

    Returns:
        List of parsed STAC item dictionaries, in the same order as keys.
    """
    semaphore = asyncio.Semaphore(64)
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()

    async def fetch(key: str) -> dict:
        async with semaphore:
            response = await loop.run_in_executor(executor, lambda: s3_client.get_object(Bucket=bucket, Key=key))
            payload = response["Body"].read()
            return json.loads(payload.decode("utf-8"))

    return await asyncio.gather(*[fetch(key) for key in keys])


def _load_stac_docs(bucket: str, keys: list[str]) -> list[dict]:
    return asyncio.run(_load_stac_docs_async(bucket, keys))


@app.command()
def index_to_stac_geoparquet(
    dataset: Literal["geomad", "lulc"] = typer.Option(..., help="Dataset type to index: 'geomad', or 'lulc'."),
    geomad_version: str = typer.Option(GEOMAD_VERSION, help="Version string for GeoMAD dataset."),
    lulc_version: str = typer.Option(LULC_VERSION, help="Version string for LULC dataset."),
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: str | None = typer.Option(None, help="Required if single_region is True."),
    single_region: bool = typer.Option(
        ...,
        help="Whether to use the single region prefix (e.g. 'dep_ls_geomad') or the generic prefix (e.g. 'ls_geomad').",
    ),
    sensor: str = typer.Option(SENSOR, help="Sensor name, e.g. 'ls' for Landsat."),
) -> None:
    """Build STAC-Geoparquet indexes from STAC items for given dataset and region(s).
    Find all STAC items across all targets and write a single combined Geoparquet.

    Reads from all prefixes, writes to a single region (e.g. dep_ls_geomad) or a generic prefix (e.g. ls_geomad)
     depending on the single_region flag.

    Args:
        dataset: Which dataset to index, e.g. 'geomad' or 'lulc'.
        region: Which region to index, e.g. 'pacific', 'non-pacific', or 'all'.
        geomad_version: Version string for GeoMAD dataset (used in S3 key paths).
        lulc_version: Version string for LULC dataset (used in S3 key paths).
        bucket: S3 bucket to read from and write to. If not provided, uses BUCKET env var.
        product_owner: Optional override for the product owner prefix (derived from region if not set).
        single_region: Whether to use the single region prefix (e.g. 'dep_ls_geomad') or the generic many-region
          prefix (e.g. 'ls_geomad').
        sensor: Sensor name, e.g. 'ls' for Landsat.
        Returns:
            The URL to the generated STAC-Geoparquet file.
    """
    bucket = bucket or get_env_var("BUCKET")  # Default
    version = version_for_dataset(dataset, geomad_version, lulc_version)

    prefixes_to_index: list[str] = []
    parquet_key: str = ""
    if single_region:
        if not product_owner:
            raise LdnError("product_owner must be provided when single_region is True.")
        prefix = build_prefix(bucket, product_owner, sensor, dataset, version)
        prefixes_to_index.append(prefix)
        parquet_key = get_stac_geoparquet_key(bucket, product_owner, sensor, dataset, version)
    else:
        for r in ["pacific", "non-pacific"]:
            r_owner = owner_for_region(r)
            prefix = build_prefix(bucket, r_owner, sensor, dataset, version)
            prefixes_to_index.append(prefix)
        # product_owner?
        parquet_key = get_stac_geoparquet_key(bucket, product_owner, sensor, dataset, version)

    logging.info(f"{len(prefixes_to_index)} prefixes to index: {prefixes_to_index}")

    # Find the regions data.
    docs_per_prefix: dict[str, list[dict]] = {}
    for prefix in prefixes_to_index:
        logger.info(f"Listing STAC items for {prefix}")
        keys = _find_stac_items_s3(bucket, prefix)
        logger.info(f"Found {len(keys)} STAC items for prefix '{prefix}'.")

        if not keys:
            logger.warning(f"No STAC items found for '{prefix}', skipping.")
            continue

        logger.info("Loading STAC items")
        docs = _load_stac_docs(bucket, keys)
        logger.info("Loaded STAC documents.")
        docs_per_prefix[prefix] = docs

    if not any(docs_per_prefix.values()):
        raise LdnError("No STAC items found under any prefix, cannot build index.")

    counts_per_region = {key: len(items) for key, items in docs_per_prefix.items()}
    logger.info(f"Is single region: {single_region}. Counts per region: {counts_per_region}")

    all_docs = [doc for docs in docs_per_prefix.values() for doc in docs]

    store = rustac.store.S3Store(bucket, credential_provider=credential_provider)  # TODO: Fix type of rustac.store
    geomad_stac_geoparquet_url = get_stac_geoparquet_url(bucket, parquet_key)
    logger.info(f"Writing combined STAC-Geoparquet ({len(all_docs)} items) to {geomad_stac_geoparquet_url}")
    write_sync(parquet_key, all_docs, store=store)
    logger.info(f"Done. Wrote {len(all_docs)} items to {geomad_stac_geoparquet_url}")


def _stac_self_link(feature: dict) -> str:
    """Extract the STAC item self-link URL."""
    links = {link["rel"]: link["href"] for link in feature.get("links", [])}
    self_link = links.get("self")
    if self_link is None:
        raise LdnError(f"Feature {feature.get('id', 'unknown')} has no self link, cannot determine STAC item URL.")
    return self_link


def _build_mosaic_for_year(year: int, item_collection: ItemCollection) -> MosaicJSON:
    """Filter features by year and build a MosaicJSON.

    Args:
        year: Year integer to filter for.
        item_collection: ItemCollection of STAC items from the index.

    Returns:
        MosaicJSON for the matching features.
    """

    def _matches_year(feat: Item) -> bool:
        """Check if a feature's datetime falls within the target year."""
        dt = feat.datetime
        if not dt:
            return False
        return dt.year == year

    year_features = [f for f in item_collection if _matches_year(f)]

    if not year_features:
        raise LdnError(f"No STAC items found for year {year}")

    def _ensure_polygon(feat: dict) -> dict:
        """Return feat with geometry as Polygon (convex hull if MultiPolygon)."""
        geom = shape(feat.get("geometry"))
        if geom.geom_type == "Polygon":
            return feat
        return {**feat, "geometry": mapping(geom.convex_hull)}

    year_items = [f.to_dict() for f in year_features]
    year_features = [_ensure_polygon(f) for f in year_items]
    logger.info(f"  {year}: {len(year_features)} features")

    mosaic = MosaicJSON.from_features(
        year_features,
        minzoom=5,
        maxzoom=12,
        accessor=_stac_self_link,
    )

    return mosaic


def _extract_years(features: ItemCollection) -> list[int]:
    """Extract sorted unique years from STAC feature datetimes.

    Args:
        features: List of STAC item features.

    Returns:
        Sorted list of year integers.
    """
    years: set[int] = set()
    for feat in features:
        dt = feat.datetime
        if dt:
            years.add(dt.year)
    return sorted(years)


@app.command()
def make_mosaics(
    dataset: Annotated[
        Literal["geomad", "lulc"],
        typer.Option(help="Which dataset to build mosaics for: 'geomad' or 'lulc'."),
    ],
    single_region: Annotated[
        bool,
        typer.Option(
            help="Whether to use the single region prefix (e.g. 'dep_ls_geomad') "
            "or the generic prefix (e.g. 'ls_geomad')."
        ),
    ],
    years: Annotated[
        str | None,
        typer.Option(
            help="Comma-separated years or range (e.g. '2020,2021' or '2010-2023'). Defaults to all years in the index."
        ),
    ] = None,
    geomad_version: Annotated[
        str,
        typer.Option(help="Version string for GeoMAD dataset."),
    ] = GEOMAD_VERSION,
    lulc_version: Annotated[
        str,
        typer.Option(help="Version string for LULC dataset."),
    ] = LULC_VERSION,
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: Annotated[
        str | None,
        typer.Option(
            help="Override the region-derived owner prefix. Optional. If using, use single_region=True."
            "Required if single_region is True."
        ),
    ] = None,
    sensor: Annotated[
        str,
        typer.Option(help="Sensor to use for the STAC item path. Optional, defaults to 'ls'."),
    ] = SENSOR,
) -> None:
    """Make mosaic.jsons per year from the combined STAC-Geoparquet index."""
    logger.info(f"Making mosaics for dataset '{dataset}'")
    bucket = bucket or get_env_var("BUCKET")  # Default
    requested_years: list[int] | None = parse_years(years) if years is not None else None
    version = version_for_dataset(dataset, geomad_version, lulc_version)

    if single_region and not product_owner:
        raise LdnError("product_owner must be provided when single_region is True.")
    stac_geoparquet_key = get_stac_geoparquet_key(bucket, product_owner, sensor, dataset, version)

    logger.info(f"Loading {'single region' if single_region else 'many regions'} index from {stac_geoparquet_key}")
    item_collection = load_stac_geoparquet_features(bucket, stac_geoparquet_key)

    available_years = _extract_years(item_collection)
    logger.info(f"Found {len(item_collection)} features across {len(available_years)} years: {available_years}")

    if requested_years is not None:
        missing = [y for y in requested_years if y not in available_years]
        if missing:
            logger.warning(f"Requested years not in index: {missing}")
        years_list = [y for y in requested_years if y in available_years]
        if not years_list:
            logger.warning("No requested years match available data, skipping.")
            return
    else:
        years_list = available_years

    # TODO: This is a bit hacky
    output_prefix = stac_geoparquet_key.rsplit("/", 1)[0]
    output_prefix = f"{output_prefix}/mosaics"

    for _year in years_list:
        mosaic = _build_mosaic_for_year(_year, item_collection)
        output_prefix_year = f"{output_prefix}/{_year}/{_year}_mosaic.json"
        body = mosaic.model_dump_json(exclude_none=True).encode("utf-8")
        s3_client.put_object(Bucket=bucket, Key=output_prefix_year, Body=body, ContentType="application/json")
        logger.info(f"  {_year} written to {output_prefix_year}.")

    logger.info("Finished writing mosaics.")
