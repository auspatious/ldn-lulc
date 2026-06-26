# Based on https://github.com/digitalearthpacific/dep-stac/blob/main/dep_collections/dep_ls_geomad.py
import json
import logging
from typing import Annotated, Literal

import typer
from pystac import (
    Collection,
    Extent,
    ItemCollection,
    Provider,
    SpatialExtent,
    Summaries,
    TemporalExtent,
)
from shapely import box, unary_union

from ldn.aws import s3_client
from ldn.utils import (
    GEOMAD_VERSION,
    LULC_VERSION,
    SENSOR,
    build_prefix,
    get_collection_url_root,
    get_env_var,
    load_stac_geoparquet_features,
    version_for_dataset,
)

collection_app = typer.Typer()
logger = logging.getLogger(__name__)


def _run_create_collection(dataset: Literal["geomad", "lulc"], extent: Extent, single_region: bool) -> Collection:
    """Create a STAC Collection for the GeoMAD or LULC dataset.

    Args:
        extent (Extent): The spatial and temporal extent of the collection.
        single_region (bool): Whether the collection is for a single region (changes path/id).

    Returns:
        Collection: A STAC Collection object.
    """
    if dataset == "geomad":
        LS_BANDS = ["red", "green", "blue", "nir08", "swir16", "swir22"]
        MAD_BANDS = [("emad", "Euclidean"), ("smad", "Spectral"), ("bcmad", "Bray-curtis")]

        collection_id = "dep_ls_geomad" if single_region else "ls_geomad"
        return Collection(
            id=collection_id,
            description="Landsat Geometric Median and Absolute Deviations (GeoMAD) over the Pacific.",
            title="Landsat GeoMAD",
            extent=extent,
            license="CC-BY-4.0",
            keywords=["Landsat", "GeoMAD", "Pacific"],
            # TODO: Is raster extension needed?
            stac_extensions=[
                "https://stac-extensions.github.io/eo/v2.0.0/schema.json",
                "https://stac-extensions.github.io/raster/v2.0.0/schema.json",
            ],
            providers=[
                # TODO: Update DEP provider for different buckets?
                Provider(
                    name="Digital Earth Pacific",
                    roles=["processor", "host"],
                    url="https://digitalearthpacific.org",
                ),
                Provider(
                    name="USGS",
                    roles=["producer", "processor", "licensor"],
                    url="https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products",
                ),
                Provider(name="NASA", roles=["licensor"], url="https://www.nasa.gov/"),
            ],
            summaries=Summaries(
                {
                    "gsd": [30],
                    # TODO: should the min/max values be in raster:bands? e.g.:
                    # "raster:bands": [
                    #     {
                    #         "name": "red",
                    #         "nodata": 0,
                    #         "statistics": {
                    #         "minimum": 0,
                    #         "maximum": 36000,
                    #         "mean": 12345,
                    #         "stddev": 4567
                    #         },
                    #         "data_type": "uint16"
                    #     }
                    # ],
                    # https://github.com/stac-extensions/eo/blob/main/examples/collection.json
                    "eo:bands": [
                        dict(
                            name=band,
                            common_name=band,  # TODO: Validate this against spec.
                            description=f"Median for {band} band",
                            min=0,
                            max=36_000,
                            nodata=0,
                        )
                        for band in LS_BANDS
                    ]
                    + [
                        dict(
                            name=band[0],
                            common_name=f"{band[1]} MAD",  # TODO: Validate this against spec.
                            description=f"{band[1]} median absolute deviations across all bands",
                            min=0,
                            max=36_000,
                            nodata=0,
                        )
                        for band in MAD_BANDS
                    ]
                    + [
                        dict(
                            name="count",
                            common_name="Count clear",  # TODO: Validate this against spec.
                            description="Count of clear observations",
                            min=0,
                            max=9998,
                            nodata=9999,
                        )
                    ],
                    "platform": ["landsat-5", "landsat-7", "landsat-8", "landsat-9"],
                },
            ),
        )
    else:
        # TODO: Make a collection for LULC.
        raise NotImplementedError(f"Collection for dataset '{dataset}' is not implemented yet.")


def calc_stac_extent(item_collection: ItemCollection) -> Extent:
    """Calculate spatial and temporal extents for the collection."""
    all_boxes = [box(*item.bbox) for item in item_collection if item.bbox is not None]
    total_bounds = unary_union(all_boxes).bounds  # (minx, miny, maxx, maxy)
    spatial_extent = SpatialExtent(bboxes=[list(total_bounds)])

    datetimes = []
    for item in item_collection:
        if item.datetime:
            datetimes.append(item.datetime)
        md = item.common_metadata
        if md.start_datetime:
            datetimes.append(md.start_datetime)
        if md.end_datetime:
            datetimes.append(md.end_datetime)

    temporal_extent = TemporalExtent(intervals=[[min(datetimes), max(datetimes)]])

    return Extent(spatial_extent, temporal_extent)


@collection_app.command()
def create_collection(
    dataset: Annotated[Literal["geomad", "lulc"], typer.Option(help="Dataset name, e.g. 'geomad' or 'lulc'.")],
    # stac_geoparquet_url: Annotated[str, typer.Option(help="URL to the STAC Geoparquet to calculate extents from.")],
    single_region: Annotated[
        bool, typer.Option(help="Whether to create a single-region collection (e.g. for Pacific only).")
    ],
    collection_url_root: Annotated[
        str | None,
        typer.Option(
            help="Optional collection URL root e.g. for a STAC API.",
        ),
    ],
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
):
    """Create and write a STAC collection JSON for the specified dataset."""
    bucket = bucket or get_env_var("BUCKET")  # Default

    if single_region and not product_owner:
        raise ValueError("product_owner must be set if single_region is True.")

    version = version_for_dataset(dataset, geomad_version, lulc_version)
    collection_url_root = collection_url_root or get_collection_url_root(
        bucket, product_owner, sensor, dataset, version
    )

    prefix = build_prefix(bucket, product_owner, sensor, dataset, version)

    item_collection = load_stac_geoparquet_features(bucket, f"{prefix}.parquet")
    extent = calc_stac_extent(item_collection)
    collection = _run_create_collection(dataset, extent, single_region)

    collection.normalize_hrefs(collection_url_root)

    key = f"{prefix}/collection.json"

    # Serialize and upload
    collection_dict = collection.to_dict()

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(collection_dict, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Wrote collection to {key}")
