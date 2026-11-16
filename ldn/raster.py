import logging
from itertools import groupby

import numpy as np
import xarray as xr
from affine import Affine
from dep_tools.utils import bbox_across_180, search_across_180
from geopandas import GeoDataFrame
from odc.geo.crs import CRS
from odc.geo.geobox import GeoBox, bounding_box_in_pixel_domain
from odc.geo.xr import assign_crs, xr_coords
from odc.stac import load as stac_load
from planetary_computer import sign_url
from pystac import ItemCollection
from pystac.extensions.projection import ProjectionExtension
from pystac_client import Client as PyStacClient
from scipy.ndimage import uniform_filter
from shapely.geometry import box, shape

from ldn.utils import (
    WGS84,
    LdnError,
)

logger = logging.getLogger(__name__)

# Landsat surface reflectance bands, and the GeoMAD median absolute deviation bands.
REFLECTANCE_BANDS = ["red", "green", "blue", "nir08", "swir16", "swir22"]
MAD_BANDS = ["emad", "smad", "bcmad"]
# GeoMAD output bands that the LULC classification pipeline needs (excludes "count").
GEOMAD_BANDS = REFLECTANCE_BANDS + MAD_BANDS
# Spectral indices added by _calculate_indices.
INDEX_BANDS = ["ndvi", "ndwi", "mndwi", "ndti", "bsi", "mbi", "baei", "bui"]

# Landsat Collection 2 scaling constants (USGS)
LANDSAT_SCALE = 0.0000275
LANDSAT_OFFSET = -0.2


def _scale_offset_landsat(data: xr.Dataset) -> xr.Dataset:
    """Scale Landsat Collection 2 reflectance values and mask nodata.

    Applies the USGS scaling formula: scaled = raw * 0.0000275 - 0.2,
    clips to [0, 1], and replaces nodata pixels with NaN.

    Modifies the dataset in place and returns it.

    Args:
        data: GeoMAD dataset loaded with odc-stac. The reflectance bands are uint16 digital numbers in the Landsat
            Collection 2 surface reflectance scale, with nodata 0 declared in the STAC items (`raster:bands`), which
            odc-stac sets as each band's `odc.nodata`. The MAD bands (float32) and count (uint16) aren't scaled.

    Returns:
        The same dataset with bands scaled to float32 in [0, 1] and nodata as NaN.

    Raises:
        LdnError: If a reflectance band has no declared nodata.
    """
    bands_to_scale = [band for band in data.data_vars if band in REFLECTANCE_BANDS]

    for band in bands_to_scale:
        raw = data[band]
        # Without a declared nodata, nodata pixels would silently become valid black (0 scales to -0.2, clipped to 0).
        if raw.odc.nodata is None:
            raise LdnError(f"Band {band} has no declared nodata. Load it with odc-stac so the STAC nodata is set.")
        nodata = raw == raw.odc.nodata
        # Reflectance <= 0 is clipped to 0: a valid very dark pixel, not nodata. Indices are NaN where every band in
        # their denominator is 0 (see _safe_divide).
        # Known limitation: over deep water, red is often slightly negative from atmospheric correction noise (e.g.
        # -0.004) while nir is about 0.004, so after clipping NDVI = (0.004 - 0) / (0.004 + 0) = 1, the value for
        # dense vegetation (24% of sea pixels on tile 033_031). Left as is: training samples are within 100 m of land,
        # so the model never learns from deep water, and the raw bands (all near 0) still identify it.
        scaled = (raw * LANDSAT_SCALE + LANDSAT_OFFSET).clip(0, 1).astype("float32")
        data[band] = scaled.where(~nodata, other=np.nan)
    return data


def _safe_divide(num: xr.DataArray, den: xr.DataArray) -> xr.DataArray:
    """Divide, giving NaN where the result is undefined (the denominator is 0) or unknown (either input is NaN).

    Input bands are clipped to [0, 1], so for an index the denominator is 0 only when every band in it is 0: no
    measurable signal. NaN marks both cases, so the pixel is treated as nodata.

    Args:
        num: Numerator.
        den: Denominator.

    Returns:
        num / den, or NaN where the denominator is 0 or either input is NaN.
    """
    defined = num.notnull() & den.notnull() & (den != 0)
    return num.where(defined) / den.where(defined)


def _calculate_indices(geomad: xr.Dataset) -> xr.Dataset:
    """Compute spectral indices from scaled geomedian bands.

    Adds index bands to the dataset in place. Bands must already be scaled and clipped to [0, 1]
    (see _scale_offset_landsat) so normalised differences stay in [-1, 1]. An index is NaN where every band in its
    denominator is 0 (no measurable signal, so the index is unknown) or an input is NaN (nodata). Pixels with a NaN
    index are nodata in prediction and dropped from training data.

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
    nir = geomad.nir08
    red = geomad.red
    green = geomad.green
    blue = geomad.blue
    swir1 = geomad.swir16
    swir2 = geomad.swir22
    # Normalised Difference Vegetation Index (Rouse 1974).
    geomad["ndvi"] = _safe_divide(nir - red, nir + red)
    # Normalised Difference Water Index (McFeeters 1996).
    geomad["ndwi"] = _safe_divide(green - nir, green + nir)
    # Modified Normalised Difference Water Index (Xu 2006).
    geomad["mndwi"] = _safe_divide(green - swir1, green + swir1)
    # Normalised Difference Turbidity Index (Lacaux 2007).
    geomad["ndti"] = _safe_divide(red - green, red + green)
    # Bare Soil Index (Rikimaru 2002).
    geomad["bsi"] = _safe_divide((swir1 + red) - (nir + blue), (swir1 + red) + (nir + blue))
    # Modified Bare soil Index (Nguyen 2021).
    geomad["mbi"] = _safe_divide(swir1 - swir2 - nir, swir1 + swir2 + nir) + 0.5
    # Built-up Area Extraction Index (Bouzekri 2015). Not a normalised difference, so not bounded to [-1, 1].
    geomad["baei"] = _safe_divide(red + 0.3, green + swir1)
    # Normalised Difference Built-up Index (Zha 2003), intermediate, not stored.
    ndbi = _safe_divide(swir1 - nir, swir1 + nir)
    # Built-Up Index (He 2010).
    geomad["bui"] = ndbi - geomad["ndvi"]
    return geomad


def _calculate_terrain_features(dem: xr.DataArray) -> xr.Dataset:
    """Compute slope, northness and eastness from an elevation DataArray on a geographic (degree) grid.

    Gradients are computed with central differences (Zevenbergen & Thorne 1987) in degrees and converted to ground
    metres with the radii of curvature of the DEM CRS's ellipsoid at each latitude: a degree of longitude shrinks
    with cos(latitude) and a degree of latitude lengthens slightly towards the poles. Working on
    the DEM's native grid avoids the scale distortion of the analysis CRSs and keeps aspect relative to true north.

    Northness and eastness are the cosine and sine of aspect (the downslope bearing) weighted by sin(slope): 1 = steep
    north/east-facing, -1 = steep south/west-facing, 0 on flat ground. Unlike raw aspect, they don't wrap from 359 to 0
    degrees, and the weighting stops DEM noise on flat ground giving a full-strength aspect.

    Args:
        dem: 2D elevation DataArray with x (longitude) and y (latitude) coordinates in degrees and a geographic CRS.

    Returns:
        Dataset with elevation, slope (degrees), northness and eastness on the same grid.
    """
    geod = dem.odc.crs.proj.get_geod()
    lat = np.radians(dem.y)
    w = 1 - geod.es * np.sin(lat) ** 2
    # Prime vertical (east-west) and meridian (north-south) radii of curvature.
    east_radius = geod.a / np.sqrt(w)
    north_radius = geod.a * (1 - geod.es) / w**1.5
    dz_dx = dem.differentiate("x") / (np.radians(1) * east_radius * np.cos(lat))
    dz_dy = dem.differentiate("y") / (np.radians(1) * north_radius)
    grad = np.hypot(dz_dx, dz_dy)
    # Equals cos/sin(aspect) * sin(slope), without computing aspect.
    hypotenuse = np.sqrt(1 + grad**2)
    terrain = {"slope": np.degrees(np.arctan(grad)), "northness": -dz_dy / hypotenuse, "eastness": -dz_dx / hypotenuse}
    # float32 like the other features: the float64 latitudes would otherwise promote these to float64.
    return xr.Dataset({"elevation": dem, **{name: value.astype("float32") for name, value in terrain.items()}})


# Focal (regional) statistics, named "{band}_{stat}_{size}" with a size x size pixel window. Candidates for model
# training, which keeps only the groups that improve accuracy. TPI is elevation minus its focal mean.
_FOCAL_SIZES = [3, 7, 15]
_TPI_SIZES = [15, 51]
FOCAL_FEATURES = (
    [f"{band}_{stat}_{size}" for band in INDEX_BANDS for stat in ["mean", "std"] for size in _FOCAL_SIZES]
    + [f"{band}_mean_{size}" for band in REFLECTANCE_BANDS for size in _FOCAL_SIZES]
    + [f"{band}_mean_7" for band in MAD_BANDS]
    + [f"elevation_tpi_{size}" for size in _TPI_SIZES]
)
# Pixels to pad each side of a tile so the widest window is complete at tile edges: half the widest window (51 x 51,
# including the focal pixel), so 25.
FOCAL_PAD = max(_FOCAL_SIZES + _TPI_SIZES) // 2

# Copernicus DEM collection on MS PC.
DEM_CATALOG = "https://planetarycomputer.microsoft.com/api/stac/v1/"
DEM_COLLECTION = "cop-dem-glo-30"
# WGS84 with its prime meridian at 180, so a grid across the antimeridian is continuous.
PM180_CRS = "+proj=longlat +datum=WGS84 +pm=180"


def load_across_antimeridian(items: ItemCollection, geobox: GeoBox, resampling: str) -> xr.Dataset:
    """Load STAC items onto a geobox that crosses the antimeridian, e.g. the DEM or LULC reference products.

    Tiles that cross the antimeridian are on the Pacific grid (EPSG:3832). Sources in WGS84 (EPSG:4326) must be
    reprojected to it, which odc-stac can't do across ±180. Measured with the DEM on tile 066_020 (Fiji):
    - stac_load(geobox=) on the tile leaves ~24% of it empty: one side of the antimeridian is missing.
    - Loading each side onto its own half of the tile geobox leaves a ~1.3 km empty strip at the antimeridian.
    - A single WGS84 load spanning both sides covers almost the whole world, which runs out of memory.

    So for geographic sources (EPSG:4326: Copernicus DEM GLO-30, ESA WorldCover and ESA CCI) this loads the east and
    west halves separately in WGS84 (_load_halves) and joins them into one continuous grid in a WGS84 CRS with its
    prime meridian at 180 (`+pm=180`, see _merge_geographic_halves), which odc.reproject handles normally. The usual
    alternative, the PROJ "+over" flag (longitudes beyond ±180), isn't supported by odc.

    Projected sources (Impact Observatory, in a UTM zone on each side, e.g. EPSG:32760 and EPSG:32601) don't wrap at
    ±180, so they load onto the geobox directly.

    Related upstream issues (no fix as of 2026-11):
    - https://github.com/opendatacube/odc-stac/issues/165
    - https://github.com/opendatacube/odc-stac/issues/172
    - https://github.com/opendatacube/odc-geo/issues/208

    Args:
        items: STAC items covering the geobox, e.g. from search_across_180.
        geobox: Target geobox in the analysis CRS, crossing the antimeridian.
        resampling: Resampling method, e.g. "bilinear" for continuous data like the DEM or "nearest" for classes like
            the land cover products.

    Returns:
        Dataset on the geobox.

    Raises:
        LdnError: If either side of the antimeridian has no items.
    """
    # The items' native CRS decides the path. Loading items in a projected CRS straight onto the geobox works (their
    # coordinates don't wrap), and splitting them would fail: Impact Observatory's UTM footprints span -180 to 180, so
    # every item would intersect both sides.
    # squeeze drops the time dimension: the items are one product for one year, so it has length 1.
    if not all(CRS(ProjectionExtension.ext(item).crs_string).geographic for item in items):
        return stac_load(items, geobox=geobox, chunks={}, resampling=resampling, patch_url=sign_url).squeeze(drop=True)
    return _merge_geographic_halves(*_load_halves(items, geobox, resampling)).odc.reproject(
        geobox, resampling=resampling
    )


def _load_halves(items: ItemCollection, geobox: GeoBox, resampling: str) -> tuple[xr.Dataset, xr.Dataset]:
    """Load the east and west sides of a geobox that crosses the antimeridian in WGS84.

    Args:
        items: EPSG:4326 STAC items covering the geobox.
        geobox: Geobox crossing the antimeridian.
        resampling: Resampling method, passed to stac_load.

    Returns:
        East and west datasets, without a time dimension.

    Raises:
        LdnError: If either side of the antimeridian has no items.
    """
    sides = [box(*bbox) for bbox in bbox_across_180(geobox)]
    # Each side loads the items whose footprint intersects it. An EPSG:4326 item's footprint can't cross ±180, so it
    # can't intersect both sides.
    east_items, west_items = ([item for item in items if shape(item.geometry).intersects(side)] for side in sides)
    if not east_items or not west_items:
        raise LdnError(
            f"Expected to load 2 halves across the antimeridian but got {len(east_items)} east and "
            f"{len(west_items)} west items. Check the tile geometry and item coverage."
        )
    east, west = (
        stac_load(
            side_items,
            geopolygon=GeoDataFrame(geometry=[side], crs=WGS84),
            chunks={},  # Force lazy.
            resampling=resampling,
            patch_url=sign_url,
        ).squeeze(drop=True)  # Remove time (length 1).
        for side_items, side in zip([east_items, west_items], sides)
    )
    return east, west


def _merge_geographic_halves(east: xr.Dataset, west: xr.Dataset) -> xr.Dataset:
    """Join WGS84 halves either side of the antimeridian into one continuous grid.

    Both halves are on the source's global pixel grid, so they're joined by pixel position: the east columns before
    the west half's first column (at -180), then the west half. Coordinates come from one GeoBox with longitudes
    relative to 180 (the east half, e.g. 179.9 to 180, becomes -0.1 to 0, and the west half, e.g. -180 to -179.9,
    becomes 0 to 0.1), in a WGS84 CRS with its prime meridian at 180. Coordinates from the two loads can't be matched
    by value: odc gives -179.99999999999997 for -180, which would leave a duplicate column.

    Args:
        east: East half, with latitude/longitude dimensions.
        west: West half, with latitude/longitude dimensions and the same rows.

    Returns:
        Merged dataset with x (longitude relative to 180) and y (latitude) dimensions and a +pm=180 CRS.

    Raises:
        ValueError: If the halves aren't on the same pixel grid or have different rows.
    """
    # Shift each half's grid to longitudes relative to 180. Only the affine changes, so no pixels move.
    east_box, west_box = (
        GeoBox(half.odc.geobox.shape, Affine.translation(shift, 0) * half.odc.geobox.affine, PM180_CRS)
        for half, shift in ((east, -180), (west, 180))
    )
    # East columns before the west half's first column, in whole pixels (rounded from e.g. 118.00000000006). Raises
    # ValueError if the halves aren't on the same pixel grid.
    west_start = bounding_box_in_pixel_domain(west_box, east_box).left
    merged_box = GeoBox((east_box.height, west_start + west_box.width), east_box.affine, PM180_CRS)
    # join="override" raises if the halves have different numbers of rows.
    merged = xr.concat([east.isel(longitude=slice(0, west_start)), west], dim="longitude", join="override")
    return assign_crs(merged.assign_coords(xr_coords(merged_box)), merged_box.crs).rename(longitude="x", latitude="y")


def _load_dem_terrain(geobox: GeoBox) -> xr.Dataset:
    """Load Copernicus DEM and compute elevation, slope, northness and eastness.

    Loads COP-DEM-GLO-30 from Planetary Computer on its native WGS84 grid (limited to the tile), computes terrain
    there (see _calculate_terrain_features), then reprojects to the target geobox with nearest resampling.

    Args:
        geobox: Target grid (of a tile) in the analysis CRS (EPSG:3832 or EPSG:6933).

    Returns:
        Dataset with elevation, slope, northness and eastness variables.
    """
    client = PyStacClient.open(DEM_CATALOG)

    # AM-crossing-safe search.
    dem_items = search_across_180(geobox, client, collections=[DEM_COLLECTION])
    logger.info(f"Found {len(dem_items)} DEM items")

    if len(dem_items) == 0:
        raise LdnError("No DEM items found. COP-DEM-GLO-30 is global so this is unexpected.")
    if len(dem_items) >= 10:
        raise LdnError(f"Too many DEM items ({len(dem_items)}). Expected ~4, data may be world-spanning.")

    dem = _load_dem_geographic(dem_items, geobox)
    logger.info(f"DEM elevation shape (native grid): {dem.shape}")
    return _calculate_terrain_features(dem).odc.reproject(geobox, resampling="nearest")


def _load_dem_geographic(dem_items: ItemCollection, geobox: GeoBox) -> xr.DataArray:
    """Load the DEM on its native WGS84 grid, limited to the geobox plus a margin for the terrain gradients.

    Args:
        dem_items: DEM STAC items covering the geobox.
        geobox: Target grid in the analysis CRS.

    Returns:
        Computed elevation DataArray with x (longitude) and y (latitude) coordinates in degrees. Antimeridian-crossing
        tiles use longitudes relative to 180 (see _merge_geographic_halves).
    """
    # 2 pixels of margin so gradients at the tile edge use real neighbours.
    area = geobox.pad(2)
    if isinstance(bbox_across_180(area), tuple):
        logger.info("Tile crosses the antimeridian, loading each side and joining them")
        dem = _merge_geographic_halves(*_load_halves(dem_items, area, "nearest"))
    else:
        dem = (
            stac_load(
                dem_items,
                geopolygon=area.footprint(WGS84),
                patch_url=sign_url,
                # Force lazy, one chunk per band. 1024 px chunks were 25-40% slower for a padded tile.
                chunks={},
            )
            .squeeze(drop=True)  # Remove time. DEM does not need a time dimension.
            .rename(longitude="x", latitude="y")
        )
    # Compute once: the DEM is lazy, and every use would otherwise download it again.
    return dem["data"].rename("elevation").compute()


def _focal_mean(values: np.ndarray, size: int) -> np.ndarray:
    """NaN-aware mean over a size x size window around each pixel.

    Args:
        values: 2D float64 array, NaN for nodata (see _focal_features for why float64).
        size: Window width in pixels.

    Returns:
        Mean of the valid pixels in each window, NaN where a window has no valid pixels.
    """
    valid = np.isfinite(values)
    total = uniform_filter(np.where(valid, values, 0.0), size, mode="constant")
    fraction_valid = uniform_filter(valid.astype("float64"), size, mode="constant")
    # Threshold at half a pixel, since the running-sum filter leaves tiny residuals in all-NaN windows.
    has_valid = fraction_valid > 0.5 / size**2
    return np.where(has_valid, total / np.where(has_valid, fraction_valid, 1), np.nan)


def _focal_features(data: xr.Dataset, names: list[str]) -> dict[str, xr.DataArray]:
    """Compute focal (regional) statistics features.

    Args:
        data: Computed (not lazy) dataset containing the source bands.
        names: Feature names in FOCAL_FEATURES, "{band}_{stat}_{size}" with stat "mean", "std" or "tpi".

    Returns:
        Mapping of feature name to float32 DataArray on the same grid as data.
    """
    features = {}
    # One band at a time, so only one band's float64 copy and focal means are in memory, and each focal mean is
    # computed once even when several features use it (e.g. ndvi_mean_7 and ndvi_std_7).
    for band, band_names in groupby(sorted(names), key=lambda name: name.rsplit("_", 2)[0]):
        # float64 because the std is E[x^2] - E[x]^2, which loses precision in float32: on a full width tile row the
        # std was off by up to 4e-4 (float64 matches a direct calculation). Results are stored as float32.
        values = data[band].values.astype("float64")
        means = {}
        for name in band_names:
            _, stat, size = name.rsplit("_", 2)
            size = int(size)
            if size not in means:
                means[size] = _focal_mean(values, size)
            if stat == "mean":
                result = means[size]
            elif stat == "std":
                result = np.sqrt(np.maximum(_focal_mean(values**2, size) - means[size] ** 2, 0))
            else:
                result = values - means[size]
            features[name] = data[band].copy(data=result.astype("float32"))
    return features


def crop_to_geobox(data: xr.Dataset | xr.DataArray, geobox: GeoBox) -> xr.Dataset | xr.DataArray:
    """Crop data to a geobox on its pixel grid, e.g. from a padded geobox back to the tile.

    Args:
        data: Dataset or DataArray on a geobox containing geobox.
        geobox: Geobox to crop to.

    Returns:
        The cropped data, a view without copying data.

    Raises:
        ValueError: If geobox isn't on the data's pixel grid.
    """
    y, x = data.odc.geobox.overlap_roi(geobox)
    return data.isel(y=y, x=x)


def make_features(geomad: xr.Dataset, features: list[str]) -> xr.Dataset:
    """Build the model features from a GeoMAD dataset: scaled bands, spectral indices, DEM terrain and focal statistics.

    Used for both training data and prediction, so the model always sees features computed the same way. Load the
    GeoMAD padded by FOCAL_PAD pixels so focal statistics are complete at tile edges, then crop_to_geobox the result.

    Args:
        geomad: Raw GeoMAD dataset for one tile and year (a single time step).
        features: Feature names to compute. Focal statistics (names in FOCAL_FEATURES) are only computed when listed;
            other names are ignored, since the base features are always computed.

    Returns:
        Computed dataset with scaled GeoMAD bands, spectral indices, elevation, slope, northness, eastness and the
        requested focal statistics on the GeoMAD grid.
    """
    data = _calculate_indices(_scale_offset_landsat(geomad.squeeze(drop=True)))

    # The DEM is reprojected by odc onto the same geobox odc-stac loaded the GeoMAD on, so their coordinates and
    # spatial_ref match exactly. join="exact" raises if they ever don't.
    dem = _load_dem_terrain(data.odc.geobox)
    # Computed because focal statistics and scikit-learn need numpy arrays, and sending a large lazy graph to Dask
    # workers is slow.
    merged = xr.merge([data, dem], join="exact", compat="no_conflicts").compute()
    return merged.assign(_focal_features(merged, [name for name in features if name in FOCAL_FEATURES]))
