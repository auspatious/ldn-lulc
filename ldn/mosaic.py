import json
import logging
from typing import Annotated, Literal

import typer
from dask.distributed import Client as DaskClient
from dep_tools.aws import object_exists
from dep_tools.grids import COUNTRIES_AND_CODES as DEP_COUNTRIES_AND_CODES
from dep_tools.namers import GenericItemPath
from odc.geo.cog import save_cog_with_dask
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from odc.stac import load as stac_load
from pystac import Asset, Item, Link, MediaType
from pystac.extensions.projection import ProjectionExtension
from rustac import search_sync

from ldn.aws import configure_s3_access_profile, s3_client
from ldn.grids import get_gadm, get_gridspec
from ldn.raster import (
    GEOMAD_BANDS,
    _add_processing_extension,
    _antimeridian_safe_bbox,
    _antimeridian_safe_geometry,
    stac_providers,
    visualisation_link,
)
from ldn.utils import (
    ALL_COUNTRIES,
    GEOMAD_DATASET_ID,
    SENSOR,
    LdnError,
    build_prefix,
    dataset_prefix,
    get_env_var,
    get_public_url_base,
    get_stac_geoparquet_key,
    get_stac_geoparquet_url,
    is_bucket_source_coop,
    owner_for_region,
)

logger = logging.getLogger(__name__)

MOSAIC_BANDS = {"geomad": GEOMAD_BANDS, "lulc": ["classification"]}
# Resampling when reading at a lower resolution and making overviews. Classes can't be averaged.
MOSAIC_RESAMPLING = {"geomad": "average", "lulc": "mode"}
RGB_BANDS = ["red", "green", "blue"]


def country_region(country: str) -> Literal["pacific", "non-pacific"]:
    """Return the grid region of a country.

    Args:
        country: ISO 3166-1 alpha-3 country code, e.g. 'FJI'.

    Raises:
        LdnError: If the country is not in the project.
    """
    if country not in ALL_COUNTRIES.values():
        raise LdnError(f"Unknown country code '{country}'. See 'ldn grid list-countries' for valid codes.")
    return "pacific" if country in DEP_COUNTRIES_AND_CODES.values() else "non-pacific"


def country_polygon(country: str, region: Literal["pacific", "non-pacific"]) -> Geometry:
    """Return a country's GADM outline in its region's grid CRS.

    The Pacific grid CRS (EPSG:3832) is continuous across the antimeridian, e.g. for Fiji.
    """
    gadm = get_gadm(countries={name: code for name, code in ALL_COUNTRIES.items() if code == country})
    return Geometry(gadm.union_all(), crs=gadm.crs).to_crs(get_gridspec(region).crs)


def mosaic_key_stem(
    country: str,
    year: str,
    dataset: Literal["geomad", "lulc"],
    version: str,
    bucket: str,
    single_region: bool,
    product_owner: str | None,
    sensor: str,
) -> str:
    """Return the S3 key of a country mosaic without its asset suffix, next to the dataset's index.

    e.g. dep_ls_geomad/0-3-0/mosaic/2020/dep_ls_geomad_FJI_2020
         auspatious/geomad-sids/ls_geomad/0-3-0/mosaic/2020/ls_geomad_FJI_2020   (Source.Coop)
    """
    owner = owner_for_region(country_region(country), product_owner) if single_region else None
    prefix = build_prefix(bucket, owner, sensor, dataset, version)
    return f"{prefix}/mosaic/{year}/{dataset_prefix(owner, sensor, dataset)}_{country}_{year}"


def mosaic_stac_item(
    stem: str,
    geobox: GeoBox,
    assets: list[str],
    input_items: list[Item],
    dataset: Literal["geomad", "lulc"],
    bucket: str,
) -> Item:
    """Build the STAC item of a country mosaic.

    Args:
        stem: S3 key of the mosaic without its asset suffix, from mosaic_key_stem.
        geobox: Geobox the mosaic was written on.
        assets: Asset names, e.g. band names and 'rgb'.
        input_items: The tile items the mosaic was made from. Their time properties are copied.
        dataset: Dataset ID, "geomad" or "lulc".
        bucket: S3 bucket the mosaic is in.

    Returns:
        STAC item with a public self href.
    """
    url = f"{get_public_url_base(bucket)}/{stem}"
    extent = geobox.geographic_extent
    item = Item(
        id=stem.rsplit("/", 1)[1],
        geometry=_antimeridian_safe_geometry(geobox, extent.json),
        bbox=_antimeridian_safe_bbox(geobox, list(extent.boundingbox)),
        datetime=input_items[0].datetime,
        properties={k: v for k, v in input_items[0].properties.items() if k in ("start_datetime", "end_datetime")},
        assets={
            name: Asset(f"{url}_{name}.tif", media_type=MediaType.COG, roles=["visual" if name == "rgb" else "data"])
            for name in assets
        },
    )
    proj = ProjectionExtension.ext(item, add_if_missing=True)
    proj.epsg = geobox.crs.epsg
    proj.shape = list(geobox.shape)
    proj.transform = list(geobox.affine)[:6]
    _add_processing_extension(
        item, ["ldn", "odc-stac", "odc-geo", "xarray"], stac_providers(dataset, is_bucket_source_coop(bucket))
    )
    item.add_link(visualisation_link())
    item.add_links(
        [
            Link(rel="derived_from", target=href, media_type="application/geo+json")
            for i in input_items
            if (href := i.get_self_href())
        ]
    )
    item.set_self_href(f"{url}.stac-item.json")
    return item


def mosaic_cog(
    country: Annotated[str, typer.Option(help="ISO 3166-1 alpha-3 country code, e.g. 'FJI'.")],
    year: Annotated[str, typer.Option(help="Year to mosaic.")],
    dataset: Annotated[Literal["geomad", "lulc"], typer.Option(help="Dataset to mosaic.")],
    version: Annotated[str, typer.Option(help="Version of the dataset, e.g. '0-3-0'.")],
    resolution: Annotated[int, typer.Option(help="Output resolution in the grid CRS units (metres).")],
    single_region: Annotated[
        bool,
        typer.Option(
            help="Whether to read the owner's single region index (e.g. 'dep_ls_geomad') "
            "or the many-region index (e.g. 'ls_geomad', as on Source.Coop).",
        ),
    ],
    bucket: Annotated[str | None, typer.Option(help="S3 bucket to read from and write to.")] = None,
    product_owner: Annotated[str | None, typer.Option(help="Override the region-derived owner prefix.")] = None,
    overwrite: Annotated[bool, typer.Option(help="Whether to overwrite an existing mosaic.")] = False,
    sensor: Annotated[str, typer.Option(help="Sensor name, e.g. 'ls' for Landsat.")] = SENSOR,
) -> None:
    """Mosaic a dataset's tiles for one country and year next to the dataset's index.

    Writes one COG per band, an RGB COG for GeoMAD, and a STAC item. The STAC item is written last, so its existence
    means the mosaic is complete.
    """
    bucket = bucket or get_env_var("BUCKET")
    region = country_region(country)
    stem = mosaic_key_stem(country, year, dataset, version, bucket, single_region, product_owner, sensor)
    stac_key = f"{stem}.stac-item.json"
    if not overwrite and object_exists(bucket, stac_key, s3_client):
        logger.info(f"Mosaic exists at {stac_key} and overwrite is False, skipping.")
        return

    owner = owner_for_region(region, product_owner)
    polygon = country_polygon(country, region)
    # Same item IDs as the dataset writer.
    itempath = GenericItemPath(sensor=sensor, dataset_id=dataset, version=version, time=year, prefix=owner)
    ids = [itempath.basename(tile) for tile, _ in get_gridspec(region).tiles_from_geopolygon(polygon)]
    index_key = get_stac_geoparquet_key(bucket, owner if single_region else None, sensor, dataset, version)
    url = get_stac_geoparquet_url(bucket, index_key)
    items = [Item.from_dict(doc) for doc in search_sync(url, ids=ids)]
    if not items:
        raise LdnError(f"No {dataset} items found for {country} {year} in {url}")
    logger.info(f"Found {len(items)} of {len(ids)} {dataset} tiles for {country} {year}.")

    geobox = GeoBox.from_geopolygon(polygon, resolution=resolution)
    resampling = MOSAIC_RESAMPLING[dataset]
    configure_s3_access_profile()  # Access must be configured here for Dask.
    with DaskClient():
        # Items in a year share a datetime, so they load as one time step.
        data = stac_load(
            items,
            bands=MOSAIC_BANDS[dataset],
            geobox=geobox,
            chunks={"x": 2048, "y": 2048},
            resampling=resampling,
            fail_on_error=True,
        ).squeeze("time", drop=True)
        arrays = {band: data[band] for band in MOSAIC_BANDS[dataset]}
        if dataset == GEOMAD_DATASET_ID:
            arrays["rgb"] = data[RGB_BANDS].to_dataarray("band").assign_attrs(nodata=data["red"].odc.nodata)
        for name, array in arrays.items():
            save_cog_with_dask(
                array, f"s3://{bucket}/{stem}_{name}.tif", blocksize=1024, overview_resampling=resampling
            ).compute()
            logger.info(f"Wrote {get_public_url_base(bucket)}/{stem}_{name}.tif")

    item = mosaic_stac_item(stem, geobox, list(arrays), items, dataset, bucket)
    s3_client.put_object(
        Bucket=bucket, Key=stac_key, Body=json.dumps(item.to_dict(), indent=2), ContentType="application/json"
    )
    logger.info(f"Wrote {item.get_self_href()}")
