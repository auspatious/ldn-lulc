import json
import logging
from typing import Annotated, Literal

import typer
from pystac import Link

from ldn.aws import s3_client
from ldn.stac import calc_stac_extent, stac_collection, visualisation_link
from ldn.utils import (
    GEOMAD_VERSION,
    LULC_VERSION,
    SENSOR,
    LdnError,
    dataset_prefix,
    get_env_var,
    get_public_url_base,
    get_stac_geoparquet_key,
    is_bucket_source_coop,
    load_stac_geoparquet_features,
    source_coop_prefix,
    version_for_dataset,
)

collection_app = typer.Typer()
logger = logging.getLogger(__name__)


@collection_app.command()
def create_collection(
    dataset: Annotated[Literal["geomad", "lulc"], typer.Option(help="Dataset name, e.g. 'geomad' or 'lulc'.")],
    single_region: Annotated[
        bool, typer.Option(help="Whether to create a single-region collection (e.g. for Pacific only).")
    ],
    url_root: Annotated[
        str | None,
        typer.Option(
            help="Optional URL root e.g. for a STAC API.",
        ),
    ] = None,
    bucket: Annotated[
        str | None, typer.Option(help="S3 bucket to read the data from and write the collection JSON to.")
    ] = None,
    sensor: Annotated[str, typer.Option(help="Sensor name, e.g. 'ls' for Landsat.")] = SENSOR,
    geomad_version: Annotated[
        str, typer.Option(help="Version of the GeoMAD data to use for calculating extents, e.g. '0-0-1'.")
    ] = GEOMAD_VERSION,
    lulc_version: Annotated[
        str, typer.Option(help="Version of the LULC data to use for calculating extents, e.g. '0-0-1'.")
    ] = LULC_VERSION,
    product_owner: Annotated[
        str | None,
        typer.Option(
            help="Must be set if single_region.",
        ),
    ] = None,
    has_stac_api: Annotated[
        bool, typer.Option(help="Whether the collection will be served via a STAC API (default: False).")
    ] = False,
):
    """Create and write a STAC collection JSON for the specified dataset."""
    bucket = bucket or get_env_var("BUCKET")  # Default

    if single_region and not product_owner:
        raise LdnError("product_owner must be provided when single_region is True.")

    version = version_for_dataset(dataset, geomad_version, lulc_version)

    public_url = url_root or get_public_url_base(bucket)
    _is_source_coop = is_bucket_source_coop(bucket)
    if _is_source_coop:
        public_url = f"{public_url}/{source_coop_prefix(dataset)}"
    _dataset_prefix = dataset_prefix(product_owner, sensor, dataset)
    collection_url_root = f"{public_url}/collections/{_dataset_prefix}"

    parquet_key = get_stac_geoparquet_key(bucket, product_owner, sensor, dataset, version)

    item_collection = load_stac_geoparquet_features(bucket, parquet_key)
    extent = calc_stac_extent(item_collection)
    collection = stac_collection(dataset, extent, single_region, _is_source_coop)

    collection.remove_links("root")
    collection.remove_links("self")
    collection.set_self_href(f"{collection_url_root}")
    collection.add_link(visualisation_link())
    if has_stac_api:
        collection.add_link(Link(rel="root", target=public_url, media_type="application/json"))
        collection.add_link(Link(rel="items", target=f"{collection_url_root}/items", media_type="application/geo+json"))

    collection.validate()

    if _is_source_coop:
        _dataset_prefix = f"{_dataset_prefix}/{source_coop_prefix(dataset)}"
    key = f"{_dataset_prefix}/collection.json"

    collection_dict = collection.to_dict()

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(collection_dict, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    if has_stac_api:
        print(f"Wrote collection to {collection_url_root}")
        # Writes: https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_geomad/collection.json
        # TODO: update https://github.com/digitalearthpacific/dep-stac/blob/main/dep_collections/dep_ls_geomad.py
    else:
        print(f"Wrote collection to {public_url}/{key}")
