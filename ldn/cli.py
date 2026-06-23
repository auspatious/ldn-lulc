import asyncio
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

import boto3
import obstore
import typer
from cogeo_mosaic.mosaic import MosaicJSON
from obstore.auth.boto3 import Boto3CredentialProvider
from pystac import ItemCollection
from rustac import search_sync, write_sync
from shapely.geometry import mapping, shape
from typing_extensions import Annotated

from ldn import get_version
from ldn.cli_geomad import geomad_app
from ldn.cli_grid import cli_grid_app
from ldn.cli_lulc import classify_app
from ldn.grids import get_grid_tiles
from ldn.raster import PrefixedS3ItemPath
from ldn.training_data import cli_training_app
from ldn.utils import (
    AWS_REGION,
    GEOMAD_VERSION,
    LULC_VERSION,
    SENSOR,
    LdnError,
    dataset_prefix,
    get_env_var,
    get_full_path_prefix,
    get_s3_mosaic_write_path,
    get_stac_geoparquet_url,
    is_bucket_source_coop,
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

aws_session = boto3.Session()  # Uses AWS_PROFILE env var automatically
s3_client = aws_session.client("s3", region_name=AWS_REGION)

# Add the subcommands
app.add_typer(cli_grid_app, name="grid", help="Commands for working with the ODC Geo Grid.")
app.add_typer(classify_app, name="lulc", help="Commands for LULC classification.")
app.add_typer(cli_training_app, name="training", help="Commands for generating training data.")
app.add_typer(geomad_app, name="geomad", help="Commands for working with GeoMAD.")


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
    suffix: str = ".stac-item.json",
    chunk_size: int = 200,
) -> list[str]:
    """List S3 keys ending in suffix under bucket/prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to search under.
        suffix: File suffix to match.
        chunk_size: Number of objects per listing page.

    Returns:
        List of S3 keys (without the s3://bucket/ prefix) that match.
    """
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
        s3_prefix = f"{dataset_prefix(region_owner, dataset_id)}/{version}/"
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
    version_geomad: Annotated[str, typer.Option(help="Version string for GeoMAD dataset.")] = GEOMAD_VERSION,
    version_lulc: Annotated[str, typer.Option(help="Version string for LULC dataset.")] = LULC_VERSION,
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: Annotated[str | None, typer.Option(help="Override the region-derived owner prefix.")] = None,
    overwrite: Annotated[bool, typer.Option(help="If true, skip filtering existing outputs.")] = False,
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
        version = version_for_dataset(dataset, version_geomad, version_lulc)
        existing = _find_existing_tasks(
            input_tasks,
            version,
            dataset,
            bucket,
            product_owner,
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
    concurrency: int = 64,
) -> list[dict]:
    """Load STAC item JSON documents from S3 concurrently.

    Args:
        bucket: S3 bucket name.
        keys: S3 object keys to load.
        concurrency: Max simultaneous S3 requests.

    Returns:
        List of parsed STAC item dictionaries, in the same order as keys.
    """
    semaphore = asyncio.Semaphore(concurrency)
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
    region: Literal["all", "pacific", "non-pacific"] = typer.Option(
        "all", help="Region to index: 'all', 'pacific', or 'non-pacific'."
    ),
    version_geomad: str = typer.Option(GEOMAD_VERSION, help="Version string for GeoMAD dataset."),
    version_lulc: str = typer.Option(LULC_VERSION, help="Version string for LULC dataset."),
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
    product_owner: str | None = typer.Option(None, help="Override the region-derived owner prefix."),
) -> None:
    """Build STAC-Geoparquet indexes from STAC items for given dataset and region(s).
    Find all STAC items across all targets and write a single combined Geoparquet."""
    regions: list[Literal["pacific", "non-pacific"]] = ["pacific", "non-pacific"] if region == "all" else [region]
    bucket = bucket or get_env_var("BUCKET")  # Default
    version = version_for_dataset(dataset, version_geomad, version_lulc)
    sc_prefix = source_coop_prefix(dataset)
    _is_bucket_source_coop = is_bucket_source_coop(bucket)

    targets: list[str] = []
    for r in regions:
        owner = owner_for_region(r, product_owner)
        prefix = f"{dataset_prefix(owner, dataset)}/{version}/"
        if _is_bucket_source_coop and sc_prefix:
            prefix = f"{sc_prefix}/{prefix}"

        targets.append(prefix)
        logger.info(f"Region for indexing: '{prefix}'")

    counts_per_region: dict[str, int] = {}
    all_docs: list = []
    for prefix in targets:
        logger.info(f"Listing STAC items for {prefix}")
        keys = _find_stac_items_s3(bucket, prefix)
        logger.info(f"Found {len(keys)} STAC items for prefix '{prefix}'.")

        if not keys:
            logger.warning(f"No STAC items found for '{prefix}', skipping.")
            continue

        logger.info("Loading STAC items")
        docs = _load_stac_docs(bucket, keys)
        logger.info("Loaded STAC documents.")
        counts_per_region[prefix] = len(docs)
        all_docs.extend(docs)

    if not all_docs:
        logger.warning("No STAC items found across any targets, skipping write.")
        return

    regions_with_data = sum(1 for count in counts_per_region.values() if count > 0)
    single_region = regions_with_data == 1  # Only one prefix has data
    logger.info(f"Is single region: {single_region}. Counts per region: {counts_per_region}")

    if single_region:
        logger.info("Single region. Writing index to the region prefix e.g. 'dep_ls_geomad'.")
    else:
        logger.info("Multiple regions. Writing combined index to the generic prefix e.g. 'ls_geomad'.")

    parquet_key = get_stac_geoparquet_url(bucket, version, dataset, single_region, just_key=True)
    # Use explicit S3Store to ensure auth works (otherwise rustac can set obstore with wrong auth).
    credential_provider = Boto3CredentialProvider(aws_session)
    store = obstore.store.S3Store(bucket=bucket, region=AWS_REGION, credential_provider=credential_provider)
    logger.info(f"Writing combined STAC-Geoparquet ({len(all_docs)} items) to {parquet_key}")
    write_sync(parquet_key, all_docs, store=store)
    logger.info(f"Done. Wrote {len(all_docs)} items to {parquet_key}")


def _stac_self_link(feature: dict) -> str:
    """Extract the STAC item self-link URL."""
    links = {link["rel"]: link["href"] for link in feature.get("links", [])}
    self_link = links.get("self")
    if self_link is None:
        raise LdnError(f"Feature {feature.get('id', 'unknown')} has no self link, cannot determine STAC item URL.")
    return self_link


def _build_mosaic_for_year(year: int, features: list[dict]) -> MosaicJSON:
    """Filter features by year and build a MosaicJSON.

    Args:
        year: Year integer to filter for.
        features: All STAC item feature dicts from the index.

    Returns:
        MosaicJSON for the matching features.
    """

    def _matches_year(feat: dict) -> bool:
        """Check if a feature's datetime falls within the target year."""
        dt_str = feat.get("properties", {}).get("datetime", "")
        if not dt_str:
            return False
        feat_year = int(dt_str[:4])
        return feat_year == year

    year_features = [f for f in features if _matches_year(f)]

    if not year_features:
        raise LdnError(f"No STAC items found for year {year}")

    def _ensure_polygon(feat: dict) -> dict:
        """Return feat with geometry as Polygon (convex hull if MultiPolygon)."""
        geom = shape(feat["geometry"])
        if geom.geom_type == "Polygon":
            return feat
        return {**feat, "geometry": mapping(geom.convex_hull)}

    year_features = [_ensure_polygon(f) for f in year_features]

    logger.info(f"  {year}: {len(year_features)} features")

    mosaic = MosaicJSON.from_features(
        year_features,
        minzoom=5,
        maxzoom=12,
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

    return features


def _extract_years(features: list[dict]) -> list[int]:
    """Extract sorted unique years from STAC feature datetimes.

    Args:
        features: List of STAC item feature dicts.

    Returns:
        Sorted list of year integers.
    """
    years: set[int] = set()
    for feat in features:
        dt_str = feat.get("properties", {}).get("datetime", "")
        if dt_str:
            years.add(int(dt_str[:4]))
    return sorted(years)


def _write_mosaic(mosaic: MosaicJSON, out_path: str) -> None:
    """Write a MosaicJSON to S3 using an explicit boto3 session."""
    # out_path is like s3://bucket/prefix/mosaic.json
    if not out_path.startswith("s3://"):
        raise LdnError(f"Output path must start with s3://, got: {out_path}")
    _, _, rest = out_path.partition("s3://")
    bucket, _, key = rest.partition("/")

    body = mosaic.model_dump_json(exclude_none=True).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


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
    version_geomad: Annotated[
        str,
        typer.Option(help="Version string for GeoMAD dataset."),
    ] = GEOMAD_VERSION,
    version_lulc: Annotated[
        str,
        typer.Option(help="Version string for LULC dataset."),
    ] = LULC_VERSION,
    bucket: Annotated[str | None, typer.Option(help="S3 bucket for data.")] = None,
) -> None:
    """Make mosaic.jsons per year from the combined STAC-Geoparquet index."""
    logger.info(f"Making mosaics for dataset '{dataset}'")
    bucket = bucket or get_env_var("BUCKET")  # Default

    requested_years: list[int] | None = parse_years(years) if years is not None else None

    version = version_for_dataset(dataset, version_geomad, version_lulc)

    parquet_url = get_stac_geoparquet_url(bucket, version, dataset, single_region)

    logger.info(f"Loading combined index from {parquet_url}")
    features = _load_all_features(parquet_url)

    if not features:
        logger.warning("No features found, skipping.")
        return

    available_years = _extract_years(features)
    logger.info(f"Found {len(features)} features across {len(available_years)} years: {available_years}")

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

    output_path = get_s3_mosaic_write_path(bucket, dataset, version)
    combined_short = dataset_prefix(None, dataset)
    for _year in years_list:
        mosaic = _build_mosaic_for_year(_year, features)
        out_path = f"{output_path}/{combined_short}_{_year}_mosaic.json"
        logger.info(f"  {_year} built successfully, writing to {out_path}")
        _write_mosaic(mosaic, out_path)
        logger.info(f"  {_year} written.")

    logger.info("Finished writing mosaics.")
