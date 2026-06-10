import logging
import sys
import json

import boto3
from dep_tools.namers import S3ItemPath
from typing_extensions import Annotated
from typing import Literal
import obstore
from rustac import write_sync

from cogeo_mosaic.backends import MosaicBackend
from cogeo_mosaic.mosaic import MosaicJSON
from pystac import ItemCollection
from rustac import search_sync
from shapely.geometry import mapping, shape

import asyncio

from ldn.grids import get_grid_tiles
import typer

from ldn import get_version
from ldn.cli_grid import cli_grid_app
from ldn.cli_classify import classify_app
from ldn.cli_geomad import geomad_app
from ldn.utils import (
    AWS_REGION,
    SOURCE_COOP_PREFIX_GEOMAD,
    SOURCE_COOP_PREFIX_PREDICTION,
    SOURCE_COOP_PUBLIC_URL,
    GEOMAD_VERSION,
    GEOMAD_DATASET_ID,
    PREDICTION_DATASET_ID,
    SENSOR,
    LdnError,
    PREDICTION_VERSION,
    BUCKET,
    PACIFIC_OWNER,
    NON_PACIFIC_OWNER,
    owner_for_region,
    dataset_prefix,
)
from ldn.training_data import cli_training_app
from ldn.aws_credentials import (
    get_write_session,
    write_credentials_as_env,
    make_obstore_s3,
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
app.add_typer(
    cli_training_app, name="training", help="Commands for generating training data."
)
app.add_typer(geomad_app, name="geomad", help="Commands for working with GeoMAD.")


# Work for version and --version
@app.command()
def version() -> None:
    """Echo the version of the software."""

    version = get_version()
    typer.echo(version)

    return


if __name__ == "__main__":
    app()


def _find_existing_tasks(
    tasks,
    version,
    dataset_id,
    bucket,
    owner_pacific,
    owner_non_pacific,
    product_owner,
):
    """Check which tasks already have outputs using S3 listing.

    Lists all STAC items under each (bucket, owner) prefix and returns
    a set of (id, year) tuples for tasks whose output already exists.
    """
    client = boto3.client("s3")

    def _full_path_prefix_for_bucket(bucket: str) -> str:
        """Return the full path prefix URL for a bucket."""
        if bucket.startswith("https://"):
            return bucket
        elif "." in bucket:
            return f"https://{bucket}"
        else:
            return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com"

    # Collect unique (bucket, owner) combos
    region_combos: set[tuple[str, str]] = set()
    for task in tasks:
        r = task["region"]
        region_combos.add(
            (
                bucket,
                owner_for_region(r, owner_pacific, owner_non_pacific, product_owner),
            )
        )

    # List all STAC items under each prefix
    existing_keys: dict[str, set[str]] = {}
    for combo_bucket, combo_owner in region_combos:
        full_prefix = dataset_prefix(combo_owner, dataset_id)
        s3_prefix = f"{full_prefix}/{version}/"
        if SOURCE_COOP_PUBLIC_URL:
            if dataset_id == GEOMAD_DATASET_ID:
                s3_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{s3_prefix}"
            elif dataset_id == PREDICTION_DATASET_ID:
                s3_prefix = f"{SOURCE_COOP_PREFIX_PREDICTION}/{s3_prefix}"
        paginator = client.get_paginator("list_objects_v2")
        key_set: set[str] = set()
        for page in paginator.paginate(Bucket=combo_bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".stac-item.json"):
                    key_set.add(key)
        existing_keys[f"{combo_bucket}/{combo_owner}"] = key_set

    total_existing = sum(len(v) for v in existing_keys.values())
    logger.info(
        f"Found {total_existing} existing STAC items in S3 (this value may be more because of input parameters to print-tasks)."
    )

    # Check each task against the set
    existing_tasks: set[tuple[str, str]] = set()
    for task in tasks:
        tile_index = tuple(map(int, task["id"].split("_")))
        r = task["region"]
        owner = owner_for_region(r, owner_pacific, owner_non_pacific, product_owner)
        full_path_prefix = _full_path_prefix_for_bucket(BUCKET)

        if SOURCE_COOP_PUBLIC_URL:
            if dataset_id == GEOMAD_DATASET_ID:
                full_path_prefix = f"{full_path_prefix}/{SOURCE_COOP_PREFIX_GEOMAD}"
            elif dataset_id == PREDICTION_DATASET_ID:
                full_path_prefix = f"{full_path_prefix}/{SOURCE_COOP_PREFIX_PREDICTION}"

        itempath = S3ItemPath(
            prefix=owner,
            bucket=BUCKET,
            sensor=SENSOR,
            dataset_id=dataset_id,
            version=version,
            time=task["year"],
            full_path_prefix=full_path_prefix,
        )
        stac_key = itempath.stac_path(tile_index, absolute=False)

        lookup_key = f"{BUCKET}/{owner}"
        if stac_key in existing_keys.get(lookup_key, set()):
            existing_tasks.add((task["id"], task["year"]))

    return existing_tasks


@app.command()
def print_tasks(
    years: Annotated[str, typer.Option()],
    region: Annotated[Literal["all", "pacific", "non-pacific"], typer.Option()] = "all",
    dataset: Annotated[
        Literal["geomad", "prediction"], typer.Option(help="Dataset name.")
    ] = "geomad",
    version_geomad: Annotated[
        str, typer.Option(help="Version string for GeoMAD dataset.")
    ] = GEOMAD_VERSION,
    version_prediction: Annotated[
        str, typer.Option(help="Version string for prediction dataset.")
    ] = PREDICTION_VERSION,
    bucket: Annotated[str, typer.Option(help="S3 bucket for Pacific data.")] = BUCKET,
    owner_pacific: Annotated[
        str,
        typer.Option(help=f"Short owner prefix for Pacific (e.g. '{PACIFIC_OWNER}')."),
    ] = PACIFIC_OWNER,
    owner_non_pacific: Annotated[
        str,
        typer.Option(
            help=f"Short owner prefix for non-Pacific (e.g. '{NON_PACIFIC_OWNER}')."
        ),
    ] = NON_PACIFIC_OWNER,
    product_owner: Annotated[
        str | None, typer.Option(help="Override the region-derived owner prefix.")
    ] = None,
    overwrite: Annotated[
        bool, typer.Option(help="If true, skip filtering existing outputs.")
    ] = False,
) -> None:
    """Print tasks for given years, optionally filtering out those with existing outputs."""
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

    # Filter out tasks whose output already exists in S3
    if not overwrite:
        dataset_id = (
            PREDICTION_DATASET_ID if dataset == "prediction" else GEOMAD_DATASET_ID
        )
        version = version_prediction if dataset == "prediction" else version_geomad

        existing = _find_existing_tasks(
            tasks,
            version,
            dataset_id,
            bucket,
            owner_pacific,
            owner_non_pacific,
            product_owner,
        )
        before_count = len(tasks)
        tasks = [t for t in tasks if (t["id"], t["year"]) not in existing]
        logger.info(
            f"Filtered: {before_count - len(tasks)} already exist, {len(tasks)} remaining."
        )
    else:
        logger.info("Overwrite enabled, skipping existence check.")

    tasks_json_str = json.dumps(tasks, separators=(",", ":"))
    sys.stdout.write(tasks_json_str)
    logger.info(f"{len(tasks)} tasks output for years: {years} and region: {region}.")
    return


# TODO: Does this overlap with find_existing_tasks?
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
    store = obstore.store.S3Store(bucket=bucket, region=AWS_REGION)
    matches: list[str] = []
    stream = obstore.list(store, prefix=prefix.lstrip("/"), chunk_size=chunk_size)

    for chunk in stream:
        for obj in chunk:
            path = obj.get("path", "")
            if path.endswith(suffix):
                matches.append(path)

    return matches


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
    store = obstore.store.S3Store(bucket=bucket, region=AWS_REGION)
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch(key: str) -> dict:
        async with semaphore:
            raw = await obstore.get_async(store, key)
            payload = raw.bytes()
            if hasattr(payload, "to_bytes"):
                payload = payload.to_bytes()
            elif not isinstance(payload, (bytes, bytearray)):
                payload = bytes(payload)
            return json.loads(payload.decode("utf-8"))

    return await asyncio.gather(*[fetch(key) for key in keys])


def _load_stac_docs(bucket: str, keys: list[str]) -> list[dict]:
    return asyncio.run(_load_stac_docs_async(bucket, keys))


@app.command()
def index_to_stac_geoparquet(
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
    bucket: str = typer.Option(BUCKET, help="S3 bucket for Pacific data."),
    owner_pacific: str = typer.Option(
        PACIFIC_OWNER, help=f"Short owner prefix for Pacific (e.g. '{PACIFIC_OWNER}')."
    ),
    owner_non_pacific: str = typer.Option(
        NON_PACIFIC_OWNER,
        help=f"Short owner prefix for non-Pacific (e.g. '{NON_PACIFIC_OWNER}').",
    ),
    product_owner: str | None = typer.Option(
        None, help="Override the region-derived owner prefix."
    ),
) -> None:
    """Build STAC-Geoparquet indexes from STAC items for given dataset(s) and region(s)."""
    targets: list[tuple[str, str, str]] = []

    regions = ["pacific", "non-pacific"] if region == "all" else [region]
    datasets = ["geomad", "prediction"] if dataset == "all" else [dataset]

    for r in regions:
        owner = owner_for_region(r, owner_pacific, owner_non_pacific, product_owner)
        for d in datasets:
            if d == GEOMAD_DATASET_ID:
                prefix = dataset_prefix(owner, GEOMAD_DATASET_ID)
                version = version_geomad
            else:
                prefix = dataset_prefix(owner, PREDICTION_DATASET_ID)
                version = version_prediction
            targets.append((bucket, prefix, version))

    for target_bucket, target_prefix, target_version in targets:
        _run_index(target_bucket, target_prefix, target_version)


def _run_index(bucket: str, prefix: str, version: str) -> None:
    """Run the STAC-Geoparquet indexing for a single bucket/prefix."""
    full_prefix = f"{prefix}/{version}"
    if SOURCE_COOP_PUBLIC_URL:  # Source.Coop prefix.
        if GEOMAD_DATASET_ID in prefix:
            full_prefix = f"{SOURCE_COOP_PREFIX_GEOMAD}/{full_prefix}"
        elif PREDICTION_DATASET_ID in prefix:
            full_prefix = f"{SOURCE_COOP_PREFIX_PREDICTION}/{full_prefix}"
    parquet_key = f"{full_prefix}/{prefix}.parquet"

    logger.info(f"Listing STAC items under s3://{bucket}/{full_prefix}")
    keys = _find_stac_items_s3(bucket, full_prefix)
    logger.info(f"Found {len(keys)} STAC items")

    if len(keys) == 0:
        logger.warning(
            f"No STAC items found under s3://{bucket}/{full_prefix}, skipping."
        )
        return

    logger.info("Loading STAC items into memory")
    docs = _load_stac_docs(bucket, keys)
    logger.info(f"Loaded {len(docs)} STAC documents")

    logger.info(f"Writing STAC-Geoparquet to s3://{bucket}/{parquet_key}")

    write_session = get_write_session()
    store = make_obstore_s3(bucket, write_session)
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
        return feat_year == int_year

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
    version_prediction: Annotated[
        str,
        typer.Option(help="Version string for prediction dataset."),
    ] = PREDICTION_VERSION,
    bucket: str = typer.Option(BUCKET, help="S3 bucket for data."),
    product_owner: Annotated[
        str | None,
        typer.Option(help="Override the region-derived owner prefix."),
    ] = None,
) -> None:
    """Make mosaic.jsons per year for GeoMedian and Prediction results from their respective STAC-Geoparquet files.

    Years are auto-detected from the STAC-Geoparquet index unless --years is provided.
    """
    logger.info(f"Making mosaics for dataset '{dataset}', region '{region}'")

    # Parse --years if provided
    requested_years: list[str] | None = None
    if years is not None:
        if "-" in years and "," not in years:
            start, end = map(int, years.split("-"))
            requested_years = [str(y) for y in range(start, end + 1)]
        else:
            requested_years = [y.strip() for y in years.split(",")]

    DATASET_CONFIG = {
        GEOMAD_DATASET_ID: {
            "version": version_geomad,
            "source_coop_prefix": SOURCE_COOP_PREFIX_GEOMAD,
        },
        PREDICTION_DATASET_ID: {
            "version": version_prediction,
            "source_coop_prefix": SOURCE_COOP_PREFIX_PREDICTION,
        },
    }

    regions = ["pacific", "non-pacific"] if region == "all" else [region]
    datasets_list = (
        [GEOMAD_DATASET_ID, PREDICTION_DATASET_ID] if dataset == "all" else [dataset]
    )

    mosaic_targets: list[tuple[str, str, str, str]] = []
    for r in regions:
        owner = owner_for_region(r, product_owner=product_owner)
        for d in datasets_list:
            cfg = DATASET_CONFIG[d]
            prefix = dataset_prefix(owner, d)
            ver = cfg["version"]
            sc_prefix = cfg["source_coop_prefix"]

            if SOURCE_COOP_PUBLIC_URL:
                read_url = f"{SOURCE_COOP_PUBLIC_URL}/{sc_prefix}"
                output_path = f"s3://{bucket}/{sc_prefix}/{prefix}/{ver}/mosaics/"
            else:
                read_url = f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}"
                output_path = f"s3://{bucket}/{prefix}/{ver}/mosaics/"

            parquet_url = f"{read_url}/{prefix}/{ver}/{prefix}.parquet"
            mosaic_targets.append((f"{d} ({r})", d, parquet_url, output_path))

    write_session = get_write_session()  # For Source.Coop credentials

    for display_name, dataset_name, stac_geoparquet_url, output_path in mosaic_targets:
        logger.info(f"Loading index for '{display_name}' from {stac_geoparquet_url}")
        features = _load_all_features(stac_geoparquet_url)

        if not features:
            logger.warning(f"No features found for '{display_name}', skipping.")
            continue

        available_years = _extract_years(features)
        logger.info(
            f"Found {len(features)} features across {len(available_years)} years: {available_years}"
        )

        if requested_years is not None:
            missing = [y for y in requested_years if y not in available_years]
            if missing:
                logger.warning(
                    f"Requested years not in index for '{display_name}': {missing}"
                )
            years_list = [y for y in requested_years if y in available_years]
            if not years_list:
                logger.warning(
                    f"No requested years match available data for '{display_name}', skipping."
                )
                continue
        else:
            years_list = available_years

        for _year in years_list:
            mosaic = _build_mosaic_for_year(_year, features)
            logger.info(f"  {_year} built successfully.")
            out_path = f"{output_path}{dataset_name}_{_year}_mosaic.json"

            with write_credentials_as_env(write_session):
                with MosaicBackend(out_path, mosaic_def=mosaic) as m:
                    m.write(overwrite=True)

            logger.info(f"  {_year} written to {out_path}")

    logger.info("Finished writing mosaics.")
