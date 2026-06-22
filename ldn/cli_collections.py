# Based on https://github.com/digitalearthpacific/dep-stac/blob/main/dep_collections/dep_ls_geomad.py
import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Literal

import boto3
import typer
from pystac import (
    Collection,
    Extent,
    Provider,
    SpatialExtent,
    Summaries,
    TemporalExtent,
)

collections_app = typer.Typer()
logger = logging.getLogger(__name__)


dep_ls_geomad_extent = Extent(
    SpatialExtent([[-180, -90, 180, 90]]),  # TODO: Update this with actual values.
    TemporalExtent([[datetime(1980, 1, 1, 0, 0, 0, 0, timezone.utc), None]]),  # TODO: Update this with actual values.
)

LS_BANDS = ["red", "green", "blue", "nir08", "swir16", "swir22"]
MAD_BANDS = [("emad", "Euclidean"), ("smad", "Spectral"), ("bcmad", "Bray-curtis")]

# TODO: Make this a function and make it more dynamic e.g. in source coop the id should be ls_geomad?
# But in DEP it should be dep_ls_geomad.
dep_ls_geomad = Collection(
    id="dep_ls_geomad",
    description="Landsat Geometric Median and Absolute Deviations (GeoMAD) over the Pacific.",
    title="Landsat GeoMAD",
    extent=dep_ls_geomad_extent,
    license="CC-BY-4.0",
    keywords=["Landsat", "GeoMAD", "Pacific"],
    providers=[
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
            "eo:bands": [
                dict(
                    name=band,
                    common_name=band,
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
                    common_name=f"{band[1]} MAD",
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
                    common_name="Count clear",
                    description="Count of clear observations",
                    min=0,
                    max=250,
                    nodata=0,
                )
            ],
            "platform": ["landsat-5", "landsat-7", "landsat-8", "landsat-9"],
        },
    ),
)

# TODO: Make a collection for LULC.


def calc_stac_extents() -> tuple[SpatialExtent, TemporalExtent]:
    """Calculate spatial and temporal extents for the collection."""
    raise NotImplementedError("TODO:")


# TODO: get spatial and temporal extent values for this collection and update the Extent above.
# Need to read all STAC items.
@collections_app.command()
def create_collection(
    dataset: Annotated[Literal["geomad", "lulc"], typer.Option(help="Dataset name, e.g. 'geomad' or 'lulc'.")],
):
    """Create a STAC collection for the specified dataset."""
    collection = None
    if dataset == "geomad":
        collection = dep_ls_geomad  # TODO: Customise this for source coop vs DEP.
    else:
        # TODO: Implement this for lulc.
        raise NotImplementedError(f"Unsupported dataset: {dataset}")

    spatial_extent, temporal_extent = calc_stac_extents()

    # Set where this collection "lives" so internal hrefs resolve correctly
    bucket = "my-bucket"
    key = "dep_ls_geomad/dep_ls_geomad.json"
    s3_uri = f"s3://{bucket}/{key}"

    # Validate hrefs/structure before writing (optional but recommended)
    collection.normalize_hrefs(f"s3://{bucket}/dep_ls_geomad")

    # Serialize and upload
    collection_dict = collection.to_dict()

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(collection_dict, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote collection to {s3_uri}")
