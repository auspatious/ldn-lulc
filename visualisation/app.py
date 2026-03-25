"""
LDN LULC Mosaic Viewer
-----------------------
Reads a STAC-Geoparquet, builds a mosaic index per year, and serves RGB
tiles from separate per-band COGs using TiTiler + STACReader.

Run:
    cd visualisation
    poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload

Map viewer (RGB):
    http://localhost:8081/mosaic/WebMercatorQuad/map.html?url=2020&assets=red&assets=green&assets=blue&rescale=5000,12000&rescale=5000,12000&rescale=5000,12000

Single band with colormap:
    http://localhost:8081/mosaic/WebMercatorQuad/map.html?url=2020&assets=red&rescale=5000,12000&colormap_name=reds
"""

# TODO: Also support single-band e.g. predicted values.

import logging
import os
import tempfile
from collections import OrderedDict
from hashlib import md5
from pathlib import Path
from typing import Annotated

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
from rustac import search_sync
from shapely.geometry import mapping, shape

from titiler.core.dependencies import AssetsExprParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.mosaic.factory import MosaicTilerFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GDAL / rasterio environment — speeds up remote COG access significantly
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STAC_GEOPARQUET_URL = (
    "https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com"
    "/ausp_ls_geomad/0-0-2/ausp_ls_geomad.parquet"
)
MOSAIC_MINZOOM = 5
MOSAIC_MAXZOOM = 14

# ---------------------------------------------------------------------------
# Build mosaic JSON files per year at startup
# ---------------------------------------------------------------------------

MOSAIC_DIR = Path(tempfile.mkdtemp(prefix="ldn_mosaics_"))
MOSAIC_PATHS: dict[str, Path] = {}


def _stac_self_link(feature: dict) -> str:
    """Extract the STAC item self-link URL."""
    links = {link["rel"]: link["href"] for link in feature.get("links", [])}
    return links.get("self", feature.get("id", ""))


def build_mosaic_for_year(year: str) -> Path:
    """Read STAC-Geoparquet, filter by year, build mosaic.json."""
    out_path = MOSAIC_DIR / f"mosaic_{year}.json"
    if out_path.exists():
        return out_path

    logger.info("Building mosaic for year %s …", year)
    item_collection = search_sync(STAC_GEOPARQUET_URL, datetime=year)
    items = ItemCollection(item_collection)
    features = [f.to_dict() for f in items]

    if not features:
        raise MosaicNotFoundError(f"No STAC items found for year {year}")

    logger.info("  %d features found", len(features))

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
        "  quadkey_zoom=%d, %d tile entries",
        mosaic.quadkey_zoom,
        len(mosaic.tiles),
    )

    with MosaicBackend(str(out_path), mosaic_def=mosaic) as m:
        m.write(overwrite=True)

    return out_path


# Pre-build mosaics for all years in the dataset
logger.info("Pre-building mosaics from %s", STAC_GEOPARQUET_URL)
# for _year in [str(y) for y in range(2000, 2025)]: # TODO: For prod reenable this.
for _year in [str(y) for y in range(2020, 2021)]: # TODO: Just for developing faster.
    try:
        MOSAIC_PATHS[_year] = build_mosaic_for_year(_year)
        logger.info("  ✓ %s", _year)
    except Exception as exc:
        logger.debug("  ✗ %s: %s", _year, exc)

logger.info("Available years: %s", sorted(MOSAIC_PATHS.keys()))


# ---------------------------------------------------------------------------
# Custom path dependency — resolve "2020" → mosaic file path
# ---------------------------------------------------------------------------


def MosaicPathParams(
    url: Annotated[
        str,
        Query(description="Year (e.g. '2020') or path to mosaic.json"),
    ],
) -> str:
    """Resolve a year string to a mosaic.json file path."""
    if url in MOSAIC_PATHS:
        return str(MOSAIC_PATHS[url])

    # Try building on-the-fly for an unknown year
    if url.isdigit() and len(url) == 4:
        try:
            path = build_mosaic_for_year(url)
            MOSAIC_PATHS[url] = path
            return str(path)
        except Exception as exc:
            raise MosaicNotFoundError(
                f"Could not build mosaic for year {url}: {exc}"
            )

    # Literal file path or URL
    return url


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="LDN LULC Mosaic Viewer",
    description=(
        "Mosaic viewer for Landsat GeoMAD data. "
        "Pass a year as `url` (e.g. `url=2020`) and band assets as "
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


# ---------------------------------------------------------------------------
# Server-side tile cache + Cache-Control headers for browser caching
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# /mosaic — STAC-backed RGB mosaic
#
# MosaicTilerFactory with STACReader reads each STAC item's per-band
# COGs and composites them into RGB tiles.
#
# The built-in map.html viewer is included by default.
# ---------------------------------------------------------------------------

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
    backend=MosaicBackend,
    dataset_reader=STACReader,
    path_dependency=MosaicPathParams,
    layer_dependency=AssetsExprParams,
    environment_dependency=lambda: GDAL_ENV,
    router_prefix="/mosaic",
)
app.include_router(mosaic_factory.router, prefix="/mosaic", tags=["Mosaic"])

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)


# ---------------------------------------------------------------------------
# Convenience endpoints
# ---------------------------------------------------------------------------


@app.get("/years", tags=["Info"])
def list_years():
    """List available years."""
    return {"years": sorted(MOSAIC_PATHS.keys())}


@app.get("/", tags=["Info"])
def root():
    """Landing page with links."""
    return {
        "title": "LDN LULC Mosaic Viewer",
        "docs": "/docs",
        "years": sorted(MOSAIC_PATHS.keys()),
        "example_rgb": (
            "/mosaic/WebMercatorQuad/map.html"
            "?url=2020"
            "&assets=red&assets=green&assets=blue"
            "&rescale=5000,12000&rescale=5000,12000&rescale=5000,12000"
        ),
        "example_single_band": (
            "/mosaic/WebMercatorQuad/map.html"
            "?url=2020"
            "&assets=red&rescale=5000,12000&colormap_name=reds"
        ),
    }
