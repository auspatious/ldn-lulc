import asyncio
import json
import logging
import sys
from typing import Literal

import boto3
import obstore
import typer
from cogeo_mosaic.mosaic import MosaicJSON
from pystac import ItemCollection
from rustac import search_sync, write_sync
from shapely.geometry import mapping, shape
from typing_extensions import Annotated

from ldn import get_version
from ldn.aws_credentials import (
    get_write_session,
    make_obstore_s3,
)
from ldn.cli_classify import classify_app
from ldn.cli_geomad import PrefixedS3ItemPath, geomad_app
from ldn.cli_grid import cli_grid_app
from ldn.grids import get_grid_tiles
from ldn.training_data import cli_training_app
from ldn.utils import (
    AWS_REGION,
    BUCKET,
    GEOMAD_DATASET_ID,
    GEOMAD_VERSION,
    NON_PACIFIC_OWNER,
    PACIFIC_OWNER,
    PREDICTION_VERSION,
    SENSOR,
    SOURCE_COOP_PREFIX_GEOMAD,
    SOURCE_COOP_PREFIX_PREDICTION,
    SOURCE_COOP_PUBLIC_URL,
    LdnError,
    dataset_prefix,
    owner_for_region,
    parse_tile_id,
    parse_years,
    resolve_dataset,
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
app.add_typer(classify_app, name="classify", help="Commands for classifying/predicting LULC.")
app.add_typer(cli_training_app, name="training", help="Commands for generating training data.")
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


def _find_stac_items_s3(
    bucket: str,
    prefix: str,
    suffix: str = ".stac-item.json",
    chunk_size: int = 200,
    public: bool = False,
) -> list[str]:
    """List S3 keys ending in suffix under bucket/prefix.

    Args:
        bucket: S3 bucket name.
        prefix: Key prefix to search under.
        suffix: File suffix to match.
        chunk_size: Number of objects per listing page.
        public: If True, skip signing (for public buckets like source.coop).

    Returns:
        List of S3 keys (without the s3://bucket/ prefix) that match.
    """
    store = obstore.store.S3Store(
        bucket=bucket,
        region=AWS_REGION,
        skip_signature=public,
    )
    matches: list[str] = []
    stream = obstore.list(store, prefix=prefix.lstrip("/"), chunk_size=chunk_size)

    for chunk in stream:
        for obj in chunk:
            path = obj.get("path", "")
            if path.endswith(suffix):
                matches.append(path)

    return matches


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
    is_public = bool(SOURCE_COOP_PUBLIC_URL)

    def _source_coop_prefix(dataset_id: str) -> str | None:
        """Return the source.coop path prefix for a dataset, or None."""
        if dataset_id == GEOMAD_DATASET_ID:
            return SOURCE_COOP_PREFIX_GEOMAD
        else:
            return SOURCE_COOP_PREFIX_PREDICTION

    sc_prefix = _source_coop_prefix(dataset_id)

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
        s3_prefix = f"{dataset_prefix(combo_owner, dataset_id)}/{version}/"
        if is_public and sc_prefix:
            s3_prefix = f"{sc_prefix}/{s3_prefix}"

        keys = _find_stac_items_s3(combo_bucket, s3_prefix, public=is_public)
        existing_keys[f"{combo_bucket}/{combo_owner}"] = set(keys)

    total_existing = sum(len(v) for v in existing_keys.values())
    logger.info(
        f"Found {total_existing} existing STAC items in S3 (this value may be more than how many are going to be "
        f"processed because of input parameters to print-tasks)."
    )

    # Check each task against the set
    existing_tasks: set[tuple[str, str]] = set()
    for task in tasks:
        tile_id_tuple = parse_tile_id(task["id"])
        r = task["region"]
        owner = owner_for_region(r, owner_pacific, owner_non_pacific, product_owner)

        full_path_prefix = f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com"
        if is_public and sc_prefix:
            full_path_prefix = f"{SOURCE_COOP_PUBLIC_URL}/{sc_prefix}"

        itempath = PrefixedS3ItemPath(
            key_prefix=sc_prefix if is_public else None,
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
    dataset: Annotated[Literal["geomad", "prediction"], typer.Option(help="Dataset name.")] = "geomad",
    version_geomad: Annotated[str, typer.Option(help="Version string for GeoMAD dataset.")] = GEOMAD_VERSION,
    version_prediction: Annotated[
        str, typer.Option(help="Version string for prediction dataset.")
    ] = PREDICTION_VERSION,
    bucket: Annotated[str, typer.Option(help="S3 bucket for data.")] = BUCKET,
    owner_pacific: Annotated[
        str,
        typer.Option(help=f"Short owner prefix for Pacific (e.g. '{PACIFIC_OWNER}')."),
    ] = PACIFIC_OWNER,
    owner_non_pacific: Annotated[
        str,
        typer.Option(help=f"Short owner prefix for non-Pacific (e.g. '{NON_PACIFIC_OWNER}')."),
    ] = NON_PACIFIC_OWNER,
    product_owner: Annotated[str | None, typer.Option(help="Override the region-derived owner prefix.")] = None,
    overwrite: Annotated[bool, typer.Option(help="If true, skip filtering existing outputs.")] = False,
) -> None:
    """Print tasks for given years, optionally filtering out those with existing outputs."""
    logger.info(f"Generating tasks for years: {years} and region: {region}")

    years_list = parse_years(years)

    tiles = get_grid_tiles(format="list", grids=region, overwrite=False)

    logger.info(f"Number of tasks: {len(years_list) * len(tiles)} (years: {len(years_list)}, tiles: {len(tiles)})")

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
        dataset_id, version, _source_coop_prefix = resolve_dataset(dataset, version_geomad, version_prediction)

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
        logger.info(f"Filtered: {before_count - len(tasks)} already exist, {len(tasks)} remaining.")
    else:
        logger.info("Overwrite enabled, skipping existence check.")

    tasks_json_str = json.dumps(tasks, separators=(",", ":"))
    sys.stdout.write(tasks_json_str)
    logger.info(f"{len(tasks)} tasks output for years: {years} and region: {region}.")
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
    dataset: Literal["geomad", "prediction"] = typer.Option(
        ..., help="Dataset type to index: 'geomad', or 'prediction'."
    ),
    region: Literal["all", "pacific", "non-pacific"] = typer.Option(
        "all", help="Region to index: 'all', 'pacific', or 'non-pacific'."
    ),
    version_geomad: str = typer.Option(GEOMAD_VERSION, help="Version string for GeoMAD dataset."),
    version_prediction: str = typer.Option(PREDICTION_VERSION, help="Version string for prediction dataset."),
    bucket: str = typer.Option(BUCKET, help="S3 bucket data."),
    owner_pacific: str = typer.Option(PACIFIC_OWNER, help=f"Short owner prefix for Pacific (e.g. '{PACIFIC_OWNER}')."),
    owner_non_pacific: str = typer.Option(
        NON_PACIFIC_OWNER,
        help=f"Short owner prefix for non-Pacific (e.g. '{NON_PACIFIC_OWNER}').",
    ),
    product_owner: str | None = typer.Option(None, help="Override the region-derived owner prefix."),
) -> None:
    """Build STAC-Geoparquet indexes from STAC items for given dataset and region(s)."""
    regions: list[Literal["pacific", "non-pacific"]] = ["pacific", "non-pacific"] if region == "all" else [region]

    dataset_id, version, source_coop_prefix = resolve_dataset(dataset, version_geomad, version_prediction)

    is_public = bool(SOURCE_COOP_PUBLIC_URL)

    targets: list[tuple[str, str]] = []
    for r in regions:
        owner = owner_for_region(r, owner_pacific, owner_non_pacific, product_owner)
        short_prefix = dataset_prefix(owner, dataset_id)
        full_prefix = f"{source_coop_prefix}/{short_prefix}/{version}" if is_public else f"{short_prefix}/{version}"
        targets.append((full_prefix, short_prefix))
        logger.info(f"Region for indexing: '{full_prefix}'")

    combined_short = f"{SENSOR}_{dataset_id}"
    parquet_key = f"/{combined_short}/{version}/{combined_short}.parquet"
    if is_public:
        parquet_key = f"{source_coop_prefix}/{parquet_key}"

    _run_index(bucket, targets, parquet_key)


def _run_index(
    bucket: str,
    targets: list[tuple[str, str]],
    parquet_key: str,
) -> None:
    """Find all STAC items across all targets and write a single combined Geoparquet."""
    all_docs: list = []

    for full_prefix, short_prefix in targets:
        logger.info(f"Listing STAC items under s3://{bucket}/{full_prefix}")
        keys = _find_stac_items_s3(bucket, full_prefix)
        logger.info(f"Found {len(keys)} STAC items under {short_prefix}")

        if not keys:
            logger.warning(f"No STAC items found under s3://{bucket}/{full_prefix}, skipping.")
            continue

        logger.info(f"Loading STAC items from {short_prefix}")
        docs = _load_stac_docs(bucket, keys)
        logger.info(f"Loaded {len(docs)} STAC documents from {short_prefix}")
        all_docs.extend(docs)

    if not all_docs:
        logger.warning("No STAC items found across any targets, skipping write.")
        return

    logger.info(f"Writing combined STAC-Geoparquet ({len(all_docs)} items) to s3://{bucket}/{parquet_key}")
    write_session = get_write_session()
    store = make_obstore_s3(bucket, write_session)
    write_sync(parquet_key, all_docs, store=store)

    logger.info(f"Done. Wrote {len(all_docs)} items to s3://{bucket}/{parquet_key}")


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


def _write_mosaic(mosaic: MosaicJSON, out_path: str, session: boto3.Session) -> None:
    """Write a MosaicJSON to S3 using an explicit boto3 session."""
    # out_path is like s3://bucket/prefix/mosaic.json
    if not out_path.startswith("s3://"):
        raise LdnError(f"Output path must start with s3://, got: {out_path}")
    _, _, rest = out_path.partition("s3://")
    bucket, _, key = rest.partition("/")

    body = mosaic.model_dump_json(exclude_none=True).encode("utf-8")
    client = session.client("s3", region_name=AWS_REGION)
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


@app.command()
def make_mosaics(
    dataset: Annotated[
        Literal["geomad", "prediction"],
        typer.Option(help="Which dataset to build mosaics for: 'geomad' or 'prediction'."),
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
    version_prediction: Annotated[
        str,
        typer.Option(help="Version string for prediction dataset."),
    ] = PREDICTION_VERSION,
    bucket: str = typer.Option(BUCKET, help="S3 bucket for data."),
) -> None:
    """Make mosaic.jsons per year from the combined STAC-Geoparquet index."""
    logger.info(f"Making mosaics for dataset '{dataset}'")

    requested_years: list[int] | None = parse_years(years) if years is not None else None

    dataset_id, version, source_coop_prefix = resolve_dataset(dataset, version_geomad, version_prediction)

    combined_short = f"{SENSOR}_{dataset_id}"

    if SOURCE_COOP_PUBLIC_URL:
        read_url = f"{SOURCE_COOP_PUBLIC_URL}/{source_coop_prefix}"
        output_path = f"s3://{bucket}/{source_coop_prefix}/{combined_short}/{version}/mosaics/"
    else:
        read_url = f"https://s3.{AWS_REGION}.amazonaws.com/{bucket}"
        output_path = f"s3://{bucket}/{combined_short}/{version}/mosaics/"

    parquet_url = f"{read_url}/{combined_short}/{version}/{combined_short}.parquet"

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

    write_session = get_write_session()

    for _year in years_list:
        mosaic = _build_mosaic_for_year(_year, features)
        out_path = f"{output_path}{combined_short}_{_year}_mosaic.json"
        logger.info(f"  {_year} built successfully, writing to {out_path}")

        _write_mosaic(mosaic, out_path, write_session)

        logger.info(f"  {_year} written.")

    logger.info("Finished writing mosaics.")
