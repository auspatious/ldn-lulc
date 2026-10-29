import json
import logging
import math
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import distribution
from typing import Literal

import numpy as np
import rioxarray  # noqa: F401  Registers the .rio accessor.
import xarray as xr
from antimeridian import fix_shape
from dep_tools.aws import BaseClient, object_exists
from dep_tools.loaders import StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor
from dep_tools.searchers import Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AreaTask
from dep_tools.utils import bbox_across_180, join_path_or_url, search_across_180
from dep_tools.writers import AwsDsCogWriter, AwsStacWriter
from geopandas import GeoDataFrame
from odc.geo.geobox import GeoBox
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import Item, ItemCollection, Link, Provider, ProviderRole
from pystac.utils import datetime_to_str
from pystac_client import Client as PyStacClient
from rasterio.enums import Resampling
from scipy.ndimage import sobel
from shapely.geometry import box

from ldn.utils import (
    WGS84,
    LdnError,
    get_public_url_base,
    source_coop_prefix,
)

logger = logging.getLogger(__name__)

# Landsat Collection 2 scaling constants (USGS)
LANDSAT_SCALE = 0.0000275
LANDSAT_OFFSET = -0.2
# Largest valid baei: max numerator (red = 1) over the smallest positive scaled reflectance (raw 7273 -> 0.0000075).
BAEI_MAX = (1 + 0.3) / (math.ceil(-LANDSAT_OFFSET / LANDSAT_SCALE) * LANDSAT_SCALE + LANDSAT_OFFSET)


def scale_offset_landsat(data: xr.Dataset) -> xr.Dataset:
    """Scale Landsat Collection 2 reflectance values and mask nodata.

    Applies the USGS scaling formula: scaled = raw * 0.0000275 - 0.2,
    clips to [0, 1], and replaces nodata pixels with NaN.

    Modifies the dataset in place and returns it.

    Args:
        data: Input dataset with Landsat integer reflectance bands.
            Expected nodata values are 0 and 65535.

    Returns:
        The same dataset with bands scaled to float32 in [0, 1].
    """
    nodata_values = (0, 65_535)

    bands_to_scale = [band for band in data.data_vars if band not in ["count", "emad", "smad", "bcmad"]]

    for band in bands_to_scale:
        raw = data[band]
        nodata = (raw == nodata_values[0]) | (raw == nodata_values[1])
        # Reflectance <= 0 is clipped to 0: a valid very dark pixel, not nodata. safe_divide handles the zeros.
        scaled = (raw * LANDSAT_SCALE + LANDSAT_OFFSET).clip(0, 1).astype("float32")
        data[band] = scaled.where(~nodata, other=np.nan)
    return data


def safe_divide(num: xr.DataArray, den: xr.DataArray, zero_value: float) -> xr.DataArray:
    """Divide, with explicit results for nodata and a zero denominator.

    Bands are clipped to [0, 1], so a denominator of 0 means every band in it is 0. That's a valid, very dark pixel
    (e.g. water in SWIR), not nodata, so it gets zero_value instead of NaN.

    Args:
        num: Numerator.
        den: Denominator.
        zero_value: Result where den is 0 (and neither input is NaN).

    Returns:
        NaN where num or den is NaN (nodata), zero_value where den is 0, otherwise num / den.
    """
    # Divide only where den isn't 0, avoiding divide by zero warnings.
    result = num / den.where(den != 0)
    # Zero denominator: a valid very dark pixel.
    result = result.where(den != 0, zero_value)
    # Either input is nodata.
    return result.where(num.notnull() & den.notnull())


def calculate_indices(geomad: xr.Dataset) -> xr.Dataset:
    """Compute spectral indices from scaled geomedian bands.

    Adds index bands to the dataset in place. Bands must already be scaled and clipped to [0, 1]
    (see scale_offset_landsat) so normalised differences stay in [-1, 1]. A zero denominator (every band
    involved is 0, a valid very dark pixel) gives 0 for normalised differences (0 / 0: no contrast) and BAEI_MAX
    for baei. NaN inputs (nodata) give NaN.

    Args:
        geomad: GeoMedian/GeoMAD dataset containing at least `nir08`, `red`, `green`,
            `blue`, `swir16`, and `swir22` bands (scaled to [0, 1]).

    Returns:
        The same dataset with additional bands: `ndvi`, `ndwi`, `mndwi`,
        `ndti`, `bsi`, `mbi`, `baei`, and `bui`.
    """
    # TODO: Check whether every index is needed. The random forest copes with correlated features, but redundant ones
    # dilute feature importance and add compute. Candidates: bui (linear combination of NDBI and ndvi, and NDBI is
    # -NDMI), ndwi (strongly anti-correlated with ndvi), mbi vs bsi (both bare soil), baei vs bui (both built-up).
    # Compare with feature importance or drop-column accuracy in notebooks/1_Train_Model.ipynb.
    # A normalised difference (a - b) / (a + b) with a + b = 0 has a = b = 0: no contrast, so 0.
    no_contrast = 0.0
    nir = geomad.nir08
    red = geomad.red
    green = geomad.green
    blue = geomad.blue
    swir1 = geomad.swir16
    swir2 = geomad.swir22
    # Normalised Difference Vegetation Index (Rouse 1974).
    geomad["ndvi"] = safe_divide(nir - red, nir + red, no_contrast)
    # Normalised Difference Water Index (McFeeters 1996).
    geomad["ndwi"] = safe_divide(green - nir, green + nir, no_contrast)
    # Modified Normalised Difference Water Index (Xu 2006).
    geomad["mndwi"] = safe_divide(green - swir1, green + swir1, no_contrast)
    # Normalised Difference Turbidity Index (Lacaux 2007).
    geomad["ndti"] = safe_divide(red - green, red + green, no_contrast)
    # Bare Soil Index (Rikimaru 2002).
    geomad["bsi"] = safe_divide((swir1 + red) - (nir + blue), (swir1 + red) + (nir + blue), no_contrast)
    # Modified Bare soil Index (Nguyen 2021).
    geomad["mbi"] = safe_divide(swir1 - swir2 - nir, swir1 + swir2 + nir, no_contrast) + 0.5
    # Built-up Area Extraction Index (Bouzekri 2015). Not a normalised difference: grows without bound as the
    # denominator approaches 0, so use the max, not 0.
    geomad["baei"] = safe_divide(red + 0.3, green + swir1, BAEI_MAX)
    # Normalised Difference Built-up Index (Zha 2003), intermediate, not stored.
    ndbi = safe_divide(swir1 - nir, swir1 + nir, no_contrast)
    # Built-Up Index (He 2010).
    geomad["bui"] = ndbi - geomad["ndvi"]
    return geomad


def _compute_terrain(dem_da: xr.DataArray) -> xr.Dataset:
    """Compute slope and aspect from an elevation DataArray.

    Uses Sobel filters to estimate terrain gradients. The pixel
    resolution is assumed to be in meters (projected CRS).

    Args:
        dem_da: 2D elevation DataArray with x/y coordinates.

    Returns:
        Dataset with elevation, slope (degrees), and aspect (degrees).
    """
    dem_vals = dem_da.values.astype("float32")
    res_m = abs(float(dem_da.x[1] - dem_da.x[0]))

    dz_dx = sobel(dem_vals, axis=1) / (8 * res_m)
    dz_dy = sobel(dem_vals, axis=0) / (8 * res_m)

    slope = xr.DataArray(
        np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))),
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="slope",
    )
    aspect = xr.DataArray(
        (90 - np.degrees(np.arctan2(-dz_dy, dz_dx))) % 360,
        coords=dem_da.coords,
        dims=dem_da.dims,
        name="aspect",
    )

    return xr.Dataset({"elevation": dem_da, "slope": slope, "aspect": aspect})


# GeoMAD output bands that the LULC classification pipeline needs (excludes "count").
GEOMAD_BANDS = [
    "nir08",
    "red",
    "green",
    "blue",
    "swir16",
    "swir22",
    "smad",
    "bcmad",
    "emad",
]

# Copernicus DEM collection on MS PC.
DEM_CATALOG = "https://planetarycomputer.microsoft.com/api/stac/v1/"
DEM_COLLECTION = "cop-dem-glo-30"


def _load_dem_am(
    dem_items: ItemCollection,
    geobox: GeoBox,
    geobox_wgs84: GeoDataFrame,
) -> xr.Dataset:
    """Load DEM for a tile that crosses the antimeridian.

    This is needed to prevent a memory error due to loading a world-spanning DEM tile when the WGS84
    footprint overlaps both sides of the AM.
    Can't load straight to target geobox CRS. Must load to WGS84, shift longitudes,
    concatenate, then reproject with +over.

    Loads east and west halves separately in WGS84, shifts west
    longitudes to >180, concatenates, and reprojects to the target
    geobox using the PROJ "+over" flag.

    The "+over" CRS flag tells PROJ to allow longitudes >180 instead
    of wrapping them. This is required because:
    - stac_load(geobox=) cannot reproject WGS84 data across the AM
      into EPSG:3832 (PROJ maps -180 to the wrong side of the projection).
    - odc.reproject does not support +over CRS (returns all NaN).
    - rioxarray's rio.reproject wraps rasterio.warp.reproject, which
      handles +over correctly.

    Related open issues (no upstream fix as of 2025-04):
    - https://github.com/opendatacube/odc-stac/issues/165
    - https://github.com/opendatacube/odc-stac/issues/172
    - https://github.com/opendatacube/odc-geo/issues/208

    Args:
        dem_items: STAC items from search_across_180.
        geobox: Target geobox in the analysis CRS (e.g. EPSG:3832).
        geobox_wgs84: Tile footprint as a WGS84 GeoDataFrame.

    Returns:
        Dataset with a single "elevation" variable in the target CRS.
    """
    east_bbox, west_bbox = bbox_across_180(geobox_wgs84)
    east_gdf = GeoDataFrame(geometry=[box(*east_bbox)], crs=WGS84)
    west_gdf = GeoDataFrame(geometry=[box(*west_bbox)], crs=WGS84)

    east_items = [i for i in dem_items if i.bbox[0] >= 0]
    west_items = [i for i in dem_items if i.bbox[0] < 0]

    halves = []
    for items, gdf in [(east_items, east_gdf), (west_items, west_gdf)]:
        if not items:
            continue
        ds = (
            stac_load(
                items,
                geopolygon=gdf,
                chunks={},  # Force lazy.
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
            ).squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
        )
        halves.append(ds)

    if len(halves) != 2:
        raise LdnError(
            f"Expected to load 2 halves of the DEM but got {len(halves)}. Check if the tile geometry is "
            f"correct and if the DEM items cover the area."
        )

    # Shift west longitudes (-180..-179) to (180..181) so the
    # two halves form a continuous longitude range.
    halves[1] = halves[1].assign_coords(longitude=(halves[1].longitude % 360))
    ds_combined = xr.concat(halves, dim="longitude").sortby("longitude")

    # rio is required here because odc.reproject does not support +over.
    ds_combined = ds_combined.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    ds_combined = ds_combined.rio.write_crs("+proj=longlat +datum=WGS84 +over")
    return ds_combined.rio.reproject(
        str(geobox.crs),
        shape=(geobox.height, geobox.width),
        transform=geobox.transform,
        resampling=Resampling.bilinear,
    ).rename({"data": "elevation"})


def load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, and aspect.

    Loads COP-DEM-GLO-30 tiles from Planetary Computer, reprojects
    to the target geobox, and derives terrain features using Sobel
    filters. Handles antimeridian-crossing tiles via _load_dem_am.

    Args:
        geobox: Target grid (of a tile) in the analysis CRS (EPSG:3832 or EPSG:6933).

    Returns:
        Dataset with elevation, slope, and aspect variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    # AM-crossing-safe search.
    dem_items = search_across_180(geobox, client, collections=[DEM_COLLECTION])
    logger.info(f"Found {len(dem_items)} DEM items")

    if len(dem_items) == 0:
        raise LdnError("No DEM items found. COP-DEM-GLO-30 is global so this is unexpected.")
    if len(dem_items) >= 10:
        raise LdnError(f"Too many DEM items ({len(dem_items)}). Expected ~4, data may be world-spanning.")

    geobox_wgs84 = GeoDataFrame(geometry=[geobox.extent.geom], crs=geobox.crs).to_crs(WGS84)
    crosses_am = isinstance(bbox_across_180(geobox_wgs84), tuple)

    if crosses_am:
        logger.info("Tile crosses the antimeridian, using custom '+over' loading logic")
        dem = _load_dem_am(dem_items, geobox, geobox_wgs84)
    else:
        logger.info("Tile does not cross the antimeridian, using standard loading logic")
        dem = (
            stac_load(
                dem_items,
                geobox=geobox,
                resampling="bilinear",
                patch_url=sign_url,
                fail_on_error=False,
                chunks={},  # Force lazy
            )
            .squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
            .rename({"data": "elevation"})
        )

    # Assign CRS so spatial_ref matches GeoMAD during xr.merge.
    dem = dem.odc.assign_crs(crs=geobox.crs)

    logger.info(f"DEM elevation shape: {dem['elevation'].shape}")

    return _compute_terrain(dem["elevation"])


def make_features(geomad: xr.Dataset) -> xr.Dataset:
    """Build the model features from a GeoMAD dataset: scaled bands, spectral indices and DEM terrain.

    Used for both training data and prediction, so the model always sees features computed the same way.

    Args:
        geomad: Raw GeoMAD dataset for one tile and year (a single time step).

    Returns:
        Dataset with scaled GeoMAD bands, spectral indices, elevation, slope and aspect on the GeoMAD grid.
    """
    data = calculate_indices(scale_offset_landsat(geomad.squeeze(drop=True)))

    # Load DEM aligned to the GeoMAD grid
    dem = load_dem_terrain(data.odc.geobox)

    # Drop spatial_ref from DEM to avoid WKT encoding conflicts with
    # the GeoMAD spatial_ref during merge (odc vs rioxarray encodings).
    if "spatial_ref" in dem.coords:
        dem = dem.drop_vars("spatial_ref")

    # Assign GeoMAD coords to DEM before merge
    dem = dem.assign_coords(x=data.x, y=data.y)
    # Override join prefers GeoMAD coords and metadata
    return xr.merge([data, dem], join="override")


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


def installed_version(name: str) -> str:
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


def add_processing_extension(item: Item, software: list[str], providers: list[Provider]) -> None:
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
    item.properties["processing:version"] = installed_version("ldn")
    item.properties["processing:software"] = {name: installed_version(name) for name in software}
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
        post_processor: Processor | None = None,
        update_item: Callable[[Item], None] | None = None,
        logger: logging.Logger = logger,
        **kwargs,
    ):
        writer = kwargs.pop("writer", AwsDsCogWriter(itempath))
        stac_creator = kwargs.pop("stac_creator", StacCreator(itempath))
        stac_writer = kwargs.pop("stac_writer", AwsStacWriter(itempath))

        super().__init__(id, area, loader, processor, writer, logger)
        self.id = id
        self.searcher = searcher
        self.post_processor = post_processor
        self.processing_software = processing_software
        self.providers = providers
        self.update_item = update_item
        self.stac_creator = stac_creator
        self.stac_writer = stac_writer

    def run(self):
        items = self.searcher.search(self.area)
        logger.info(f"Found {len(items)} items for this tile/year")
        input_hrefs = [href for item in items if (href := item.get_self_href())]
        input_data = self.loader.load(items, self.area)
        logger.info(f"Loaded {len(input_data.time.values)} items for this tile/year")

        processor_kwargs = dict(area=self.area) if self.processor.send_area_to_processor else dict()
        output_data = self.processor.process(input_data, **processor_kwargs)

        if self.post_processor is not None:
            output_data = self.post_processor.process(output_data)

        paths = self.writer.write(output_data, self.id)

        if self.stac_creator is not None and self.stac_writer is not None:
            stac_item = self.stac_creator.process(output_data, self.id)
            stac_item.bbox = _antimeridian_safe_bbox(self.area, stac_item.bbox)
            stac_item.geometry = _antimeridian_safe_geometry(self.area, stac_item.geometry)
            add_processing_extension(stac_item, self.processing_software, self.providers)
            stac_item.add_link(visualisation_link())
            # Link to the input items (Landsat scenes for GeoMAD, the GeoMAD item for LULC).
            stac_item.add_links(
                [Link(rel="derived_from", target=href, media_type="application/geo+json") for href in input_hrefs]
            )
            if self.update_item is not None:
                self.update_item(stac_item)
            self.stac_writer.write(stac_item, self.id)

        return paths
