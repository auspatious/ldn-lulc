import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import distribution
from typing import Literal

from antimeridian import fix_shape
from dep_tools.aws import BaseClient, object_exists
from dep_tools.loaders import StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AreaTask
from dep_tools.utils import bbox_across_180, join_path_or_url
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from odc.geo.geobox import GeoBox
from pystac import Item, ItemCollection, Link, Provider, ProviderRole
from pystac.utils import datetime_to_str

from ldn.utils import get_public_url_base, source_coop_prefix

logger = logging.getLogger(__name__)


# This is needed to support Source.Coop prefix.
# PrefixedS3ItemPath needs the key_prefix thing for Source.Coop.
class PrefixedS3ItemPath(S3ItemPath):
    def __init__(self, key_prefix: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.key_prefix = key_prefix.strip("/") if key_prefix else None

    def path(self, item_id, asset_name=None, ext=".tif", absolute=False) -> str:
        relative_path = super().path(item_id, asset_name=asset_name, ext=ext, absolute=False)
        if self.key_prefix:
            relative_path = f"{self.key_prefix}/{relative_path}"
        return (
            join_path_or_url(self.full_path_prefix, relative_path)
            if absolute and self.full_path_prefix is not None
            else relative_path
        )


# A shared function to create tasks for Geomad and Classification.
def build_pipeline_components(
    tile_id_tuple: tuple[int, int],
    year: str,
    version: str,
    bucket: str,
    owner: str,
    dataset_id: Literal["geomad", "lulc"],
    source_coop_prefix: str | None,
    overwrite: bool,
    collection_url_root: str,
    s3_client: BaseClient,
    sensor: str,
) -> tuple[PrefixedS3ItemPath, StacCreator, AwsDsCogWriter] | None:
    """Build shared pipeline components for GeoMAD and classify tasks.

    Returns None if the item already exists and overwrite is False.
    """
    full_path_prefix = get_public_url_base(bucket)

    itempath = PrefixedS3ItemPath(
        key_prefix=source_coop_prefix,
        prefix=owner,
        bucket=bucket,
        sensor=sensor,
        dataset_id=dataset_id,
        version=version,
        time=year,
        full_path_prefix=full_path_prefix,
    )
    stac_document = itempath.stac_path(tile_id_tuple, absolute=True)
    stac_key = itempath.stac_path(tile_id_tuple, absolute=False)

    logger.info(f"Checking if item exists at {stac_document} with overwrite={overwrite}")
    if not overwrite and object_exists(bucket, stac_key):
        logger.info(f"Skipping because item already exists at {stac_document};")
        return None
    logger.info("Either item does not exist or overwrite is True, proceeding with processing.")

    # Items use the projection and raster extensions at v1.1.0, not their latest versions, because rio-stac (used by
    # StacCreator) hard-codes them: https://github.com/developmentseed/rio-stac/issues/64
    stac_creator = StacCreator(
        collection_url_root=collection_url_root,
        itempath=itempath,
        with_raster=True,
    )
    writer = AwsDsCogWriter(
        itempath,
        write_multithreaded=True,
        client=s3_client,
    )

    return itempath, stac_creator, writer


def _antimeridian_safe_bbox(area: GeoBox, fallback_bbox: list[float]) -> list[float]:
    """Return a STAC-correct bbox for a tile, fixing antimeridian-crossing cases.

    rio_stac derives the STAC bbox from the raster's own reprojected corners,
    which for a tile crossing the antimeridian collapses to a near-global
    (-180, 180) box instead of the STAC-recommended [east_edge, ymin,
    west_edge, ymax] form (bbox[0] > bbox[2]). `bbox_across_180` already knows
    how to split such a tile into two non-crossing halves; here we just glue
    the outer edges of those halves back into one antimeridian-flipped bbox.
    """
    halves = bbox_across_180(area)
    if not isinstance(halves, tuple):
        return fallback_bbox
    east_bbox, west_bbox = halves
    return [
        east_bbox[0],
        min(east_bbox[1], west_bbox[1]),
        west_bbox[2],
        max(east_bbox[3], west_bbox[3]),
    ]


def _antimeridian_safe_geometry(area: GeoBox, geometry: dict) -> dict:
    """Split a STAC geometry at the antimeridian, only if the area crosses it.

    antimeridian.fix_shape mangles small polygons next to the antimeridian that don't cross it (e.g. the 5 x 5 pixel
    integration test corner of tile 066_020 raises "Too few points in geometry component"), so it's only applied to
    areas that cross.

    Args:
        area: The tile or test area the geometry covers.
        geometry: GeoJSON geometry from rio_stac.

    Returns:
        A GeoJSON MultiPolygon split at the antimeridian if the area crosses it, otherwise the geometry unchanged.
    """
    return fix_shape(geometry) if isinstance(bbox_across_180(area), tuple) else geometry


# TODO: Use pystac's processing extension (and validate items with it) once the extension is supported by pystac
# https://github.com/stac-extensions/processing
# https://github.com/stac-utils/pystac/pull/1589
# https://github.com/stac-utils/pystac/pull/1785
PROCESSING_EXTENSION = "https://stac-extensions.github.io/processing/v1.3.0/schema.json"
# Processing was done by Auspatious on Digital Earth Pacific infrastructure.
PROCESSING_FACILITY = "Digital Earth Pacific"


def visualisation_link() -> Link:
    """A link to the LDN LULC visualisation app, for STAC items and collections.

    Returns:
        A STAC Link.
    """
    return Link(
        rel="related", target="https://auspatious.github.io/ldn-lulc", media_type="text/html", title="Visualisation"
    )


# Processing level and lineage of each dataset, recorded on the Auspatious provider.
PROCESSING_LEVEL_LINEAGE = {
    "geomad": {
        "processing:level": "L3",
        "processing:lineage": "Annual geometric median and median absolute deviations of Landsat Collection 2 Level-2 "
        "surface reflectance",
    },
    "lulc": {
        "processing:level": "L4",
        "processing:lineage": "Random forest land use/land cover classification of annual Landsat GeoMAD, spectral "
        "indices and Copernicus DEM terrain, trained on the agreement of ESA WorldCover, ESA CCI and Impact "
        "Observatory land cover in 2020",
    },
}


def stac_providers(dataset: Literal["geomad", "lulc"], source_coop: bool) -> list[Provider]:
    """STAC providers for a dataset, shared by its collection and items so items describe themselves.

    Digital Earth Pacific is always a processor, and the host unless the data is on Source Cooperative. Auspatious
    is the processor on Digital Earth Pacific infrastructure, with the dataset's processing level and lineage.

    Args:
        dataset: Dataset ID, "geomad" or "lulc".
        source_coop: Whether the data is hosted on Source Cooperative.

    Returns:
        List of STAC Providers.
    """
    return [
        Provider(
            name="Digital Earth Pacific",
            roles=[ProviderRole.PROCESSOR] if source_coop else [ProviderRole.PROCESSOR, ProviderRole.HOST],
            url="https://digitalearthpacific.org",
        ),
        *(
            [
                Provider(
                    name="Source Cooperative",
                    roles=[ProviderRole.HOST],
                    url=f"https://source.coop/{source_coop_prefix(dataset)}",
                )
            ]
            if source_coop
            else []
        ),
        Provider(
            name="Auspatious",
            roles=[ProviderRole.PROCESSOR],
            url="https://auspatious.com",
            extra_fields={"processing:facility": PROCESSING_FACILITY, **PROCESSING_LEVEL_LINEAGE[dataset]},
        ),
        Provider(
            name="USGS",
            roles=[ProviderRole.PRODUCER, ProviderRole.PROCESSOR, ProviderRole.LICENSOR],
            url="https://www.usgs.gov/landsat-missions/landsat-collection-2-level-2-science-products",
        ),
        Provider(name="NASA", roles=[ProviderRole.PRODUCER, ProviderRole.LICENSOR], url="https://www.nasa.gov/"),
    ]


def _installed_version(name: str) -> str:
    """Return the installed version of a package, with the commit appended if it was installed from git.

    Args:
        name: Installed package name.

    Returns:
        Version string, e.g. "2.5.2" or "0.5.4+fb8667b40bff46caf34193fe522b35f607020196".
    """
    dist = distribution(name)
    direct_url = json.loads(dist.read_text("direct_url.json") or "{}")
    commit = direct_url.get("vcs_info", {}).get("commit_id")
    return f"{dist.version}+{commit}" if commit else dist.version


def _add_processing_extension(item: Item, software: list[str], providers: list[Provider]) -> None:
    """Add processing extension fields and providers to a STAC item in place.

    Records the ldn version (processing:version), software versions, the processing datetime in UTC, and the
    providers (see stac_providers).

    Args:
        item: STAC item to update.
        software: Installed package names whose versions are recorded in `processing:software`.
        providers: STAC providers, from stac_providers.
    """
    if PROCESSING_EXTENSION not in item.stac_extensions:
        item.stac_extensions.append(PROCESSING_EXTENSION)
    item.properties["processing:version"] = _installed_version("ldn")
    item.properties["processing:software"] = {name: _installed_version(name) for name in software}
    item.properties["processing:datetime"] = datetime_to_str(datetime.now(UTC))
    item.common_metadata.providers = providers


# Shared by both GeoMAD creation and LULC classification tasks.
class AwsStacTask(AreaTask):
    """Area task with search + STAC creation/writing for AWS workflows."""

    def __init__(
        self,
        itempath: PrefixedS3ItemPath,
        id: tuple[int, int],
        area: GeoBox,
        searcher: Searcher,
        loader: StacLoader,
        processor: Processor,
        processing_software: list[str],
        providers: list[Provider],
        writer: AwsDsCogWriter,
        stac_creator: StacCreator,
        update_item: Callable[[Item], None] | None = None,
        used_items: Callable[[ItemCollection], list[Item]] | None = None,
        logger: logging.Logger = logger,
    ):
        super().__init__(id, area, loader, processor, writer, logger)
        self.id = id
        self.searcher = searcher
        self.processing_software = processing_software
        self.providers = providers
        self.update_item = update_item
        self.used_items = used_items
        self.stac_creator = stac_creator
        self.stac_writer = AwsStacWriter(itempath)

    def run(self):
        items = self.searcher.search(self.area)
        logger.info(f"Found {len(items)} items for this tile/year")
        input_data = self.loader.load(items, self.area)
        logger.info(f"Loaded {len(input_data.time.values)} items for this tile/year")

        output_data = self.processor.process(input_data)
        paths = self.writer.write(output_data, self.id)

        stac_item = self.stac_creator.process(output_data, self.id)
        stac_item.bbox = _antimeridian_safe_bbox(self.area, stac_item.bbox)
        stac_item.geometry = _antimeridian_safe_geometry(self.area, stac_item.geometry)
        _add_processing_extension(stac_item, self.processing_software, self.providers)
        stac_item.add_link(visualisation_link())
        # Link to the input items used (Landsat scenes for GeoMAD, GeoMAD items for LULC). Called after processing,
        # because only the processor knows which items it used.
        used = items if self.used_items is None else self.used_items(items)
        input_hrefs = [href for item in used if (href := item.get_self_href())]
        stac_item.add_links(
            [Link(rel="derived_from", target=href, media_type="application/geo+json") for href in input_hrefs]
        )
        if self.update_item is not None:
            self.update_item(stac_item)
        self.stac_writer.write(stac_item, self.id)

        return paths
