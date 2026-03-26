"""
LDN GeoMedian/GeoMAD and Predicted LULC Mosaic Viewer
-----------------------
Reads a STAC-Geoparquet (either GeoMedian/GeoMAD or predicted LULC), builds a mosaic index per year, and serves RGB or single band
tiles from separate per-band COGs using TiTiler + STACReader.

Run:
    cd visualisation
    poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload

Map viewer GeoMedian RGB:
    http://localhost:8081/mosaic/WebMercatorQuad/map.html?dataset=geomad&year=2020&assets=red&assets=green&assets=blue&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500

Predicted LULC:
    http://localhost:8081/mosaic/WebMercatorQuad/map.html?dataset=prediction&year=2020&assets=lucl&colormap_name=lulc
"""


import logging
import os
import tempfile
from collections import OrderedDict
from hashlib import md5
from pathlib import Path
from typing import Annotated, Literal

from cogeo_mosaic.backends import MosaicBackend
from cogeo_mosaic.errors import MosaicNotFoundError
from cogeo_mosaic.mosaic import MosaicJSON
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from pystac import ItemCollection
from rio_tiler.io import STACReader
from rio_tiler.colormap import cmap
from rustac import search_sync
from shapely.geometry import mapping, shape

from titiler.core.dependencies import AssetsExprParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.mosaic.factory import MosaicTilerFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LUCL_COLORMAP = {
    0: (255, 255, 255, 0),    # No data    — white, transparent
    1: (0,   100, 0,   255),  # Tree Cover — darkgreen
    2: (50,  205, 50,  255),  # Grassland  — limegreen
    3: (0,   255, 0,   255),  # Cropland   — lime
    4: (64,  224, 208, 255),  # Wetland    — turquoise
    5: (128, 128, 128, 255),  # Built-up   — gray
    6: (0,   0,   255, 255),  # Water      — blue
    7: (255, 255, 0,   255),  # Other      — yellow
}
cmap.register({"lulc": LUCL_COLORMAP})

# GDAL / rasterio environment — speeds up remote COG access significantly
os.environ.update(
    {
        # GDAL HTTP settings
        "GDAL_HTTP_MULTIPLEX": "YES",
        "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
        "GDAL_HTTP_MAX_RETRY": "3",
        "GDAL_HTTP_RETRY_DELAY": "1",
        # VSI caching — avoids re-fetching headers/overviews
        "VSI_CACHE": "TRUE",
        "VSI_CACHE_SIZE": "536870912",  # 512 MB
        "GDAL_CACHEMAX": "512",  # 512 MB raster block cache
        # Band interleaving optimisation
        "GDAL_BAND_BLOCK_CACHE": "HASHSET",
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        # Concurrency — keep connections alive
        "GDAL_HTTP_TCP_KEEPALIVE": "YES",
        # Mosaic concurrency — parallel reads of assets within a tile
        "MOSAIC_CONCURRENCY": "8",
    }
)


# Configuration
STAC_GEOPARQUET_URL_GEOMAD = (
    "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com"
    "/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet"
)
STAC_GEOPARQUET_URL_PREDICTION = (
    "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com"
    "/ausp_ls_prediction/0-0-1/ausp_ls_prediction.parquet"
)
MOSAIC_MINZOOM = 5
MOSAIC_MAXZOOM = 14


# Build mosaic JSON files per dataset and year at startup
MOSAIC_DIR = Path(tempfile.mkdtemp(prefix="ldn_mosaics"))
MOSAIC_PATHS_GEOMAD: dict[str, Path] = {}
MOSAIC_PATHS_PREDICTION: dict[str, Path] = {}

datasets = [
    ("prediction", STAC_GEOPARQUET_URL_PREDICTION, MOSAIC_PATHS_PREDICTION),
    ("geomad", STAC_GEOPARQUET_URL_GEOMAD, MOSAIC_PATHS_GEOMAD),
]


def _stac_self_link(feature: dict) -> str:
    """Extract the STAC item self-link URL."""
    links = {link["rel"]: link["href"] for link in feature.get("links", [])}
    return links.get("self", feature.get("id", ""))


def build_mosaic_for_year(dataset_name: str, year: str, stac_geoparquet_url: str) -> Path:
    """Read STAC-Geoparquet, filter by year, build mosaic.json."""
    out_path = MOSAIC_DIR / dataset_name / f"mosaic_{year}.json"

    # This never occurs?
    # if out_path.exists():
    #     return out_path

    # Ensure the dataset subdirectory exists
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Building mosaic for year {year}")
    item_collection = search_sync(stac_geoparquet_url, datetime=year)
    items = ItemCollection(item_collection)
    features = [f.to_dict() for f in items]

    if not features:
        raise MosaicNotFoundError(f"No STAC items found for year {year}")

    logger.info(f"  {len(features)} features found")

    # cogeo-mosaic requires Polygon geometries
    for feat in features:
        geom = shape(feat["geometry"])
        if geom.geom_type != "Polygon":
            geom = geom.convex_hull
        feat["geometry"] = mapping(geom)

    mosaic = MosaicJSON.from_features(
        features,
        minzoom=MOSAIC_MINZOOM,
        maxzoom=MOSAIC_MAXZOOM,
        accessor=_stac_self_link,
    )

    logger.info(
        f"  quadkey_zoom={mosaic.quadkey_zoom}, {len(mosaic.tiles)} tile entries"
    )

    with MosaicBackend(str(out_path), mosaic_def=mosaic) as m:
        m.write(overwrite=True)

    return out_path


# Pre-build mosaics for all years in the dataset
for (dataset_name, stac_geoparquet_url, mosaic_paths) in datasets:
  logger.info(f"Pre-building mosaics for '{dataset_name}' dataset.")
  # for _year in [str(y) for y in range(2000, 2025)]: # TODO: For prod reenable this.
  for _year in [str(y) for y in range(2020, 2021)]: # TODO: Just for developing faster.
      mosaic_paths[_year] = build_mosaic_for_year(dataset_name, _year, stac_geoparquet_url)
      logger.info(f"  {_year} built successfully.")

  logger.info(f"Available years for '{dataset_name}': {sorted(mosaic_paths.keys())}")


# Custom path dependency — resolve "2020" to mosaic file path
def MosaicPathParams(
    year: Annotated[
        str,
        Query(description="Year (e.g. '2020') or path to mosaic.json"),
    ],
    dataset: Annotated[
        Literal["geomad", "prediction"],
        Query(description="Dataset name (e.g. 'geomad' or 'prediction')"),
     ],
) -> str:
    """Resolve dataset and year query parameters to a mosaic.json file path."""
    dataset_set = next((d for d in datasets if d[0] == dataset), None)
    if not dataset_set:
        raise MosaicNotFoundError(f"Unknown dataset '{dataset}'. Valid options: {[d[0] for d in datasets]}")
    
    mocasic_paths = dataset_set[2]
    if year in mocasic_paths:
        return str(mocasic_paths[year])
    else:
        raise MosaicNotFoundError(f"No mosaic found for year '{year}' in dataset '{dataset}'. Available years: {sorted(mocasic_paths.keys())}")


# FastAPI app
app = FastAPI(
    title="LDN LULC Mosaic Viewer",
    description=(
        "Mosaic viewer for Landsat GeoMedian/GeoMAD and LULC Prediction data. "
        "Pass a dataset as `dataset` (e.g. `dataset=geomad` or `dataset=prediction`) and year as `year` (e.g. `year=2020`) and band assets as "
        "`assets=red&assets=green&assets=blue`."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)



# Server-side tile cache + Cache-Control headers for browser caching
_TILE_CACHE: OrderedDict[str, tuple[bytes, str, dict]] = OrderedDict()
_TILE_CACHE_MAX = 2048  # max cached tiles (~2k × ~50KB ≈ 100 MB)


class TileCacheMiddleware(BaseHTTPMiddleware):
    """In-memory LRU cache for tile responses + Cache-Control headers."""

    async def dispatch(self, request: Request, call_next):
        is_tile = "/tiles/" in request.url.path
        cache_key = None

        if is_tile:
            cache_key = md5(str(request.url).encode()).hexdigest()
            if cache_key in _TILE_CACHE:
                body, media_type, headers = _TILE_CACHE[cache_key]
                _TILE_CACHE.move_to_end(cache_key)  # LRU refresh
                return Response(
                    content=body,
                    media_type=media_type,
                    headers={**headers, "X-Tile-Cache": "HIT"},
                )

        response = await call_next(request)

        # Add Cache-Control header for tile responses so browsers cache them
        if is_tile and response.status_code == 200 and cache_key:
            response.headers["Cache-Control"] = "public, max-age=86400"

            # Buffer and cache the response body
            body = b""
            async for chunk in response.body_iterator:  # type: ignore[union-attr]
                body += chunk
            media_type = response.media_type or "image/png"
            headers = dict(response.headers)
            headers["X-Tile-Cache"] = "MISS"

            _TILE_CACHE[cache_key] = (body, media_type, headers)
            if len(_TILE_CACHE) > _TILE_CACHE_MAX:
                _TILE_CACHE.popitem(last=False)  # evict oldest

            return Response(content=body, media_type=media_type, headers=headers)

        return response


app.add_middleware(TileCacheMiddleware)


# /mosaic — STAC-backed RGB mosaic
#
# MosaicTilerFactory with STACReader reads each STAC item's per-band
# COGs and composites them into RGB tiles.
#
# The built-in map.html viewer is included by default.
GDAL_ENV = {
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": 536_870_912,
    "GDAL_CACHEMAX": 512,
    "GDAL_BAND_BLOCK_CACHE": "HASHSET",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
}

mosaic_factory = MosaicTilerFactory(
    backend=MosaicBackend, # type: ignore
    dataset_reader=STACReader,
    path_dependency=MosaicPathParams,
    layer_dependency=AssetsExprParams,
    environment_dependency=lambda: GDAL_ENV,
    router_prefix="/mosaic",
)
app.include_router(mosaic_factory.router, prefix="/mosaic", tags=["Mosaic"])

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)



# Convenience endpoints
@app.get("/years-geomad", tags=["Info"])
def list_years_geomad():
    """List available years for GeoMAD."""
    return {"years": sorted(MOSAIC_PATHS_GEOMAD.keys())}

@app.get("/years-prediction", tags=["Info"])
def list_years_prediction():
    """List available years for prediction."""
    return {"years": sorted(MOSAIC_PATHS_PREDICTION.keys())}


@app.get("/", tags=["Info"])
def root():
    """Landing page with links."""
    print(MOSAIC_PATHS_GEOMAD)
    return {
        "title": "LDN LULC Mosaic Viewer",
        "docs": "/docs",
        "years-geomad": sorted(MOSAIC_PATHS_GEOMAD.keys()),
        "years-prediction": sorted(MOSAIC_PATHS_PREDICTION.keys()),
        "example_geomad": (
            "/mosaic/WebMercatorQuad/map.html?dataset=geomad&year=2020&assets=red&assets=green&assets=blue&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500"
        ),
        "example_prediction": (
            "/mosaic/WebMercatorQuad/map.html?dataset=prediction&year=2020&assets=lucl&colormap_name=lulc"
        ),
    }
