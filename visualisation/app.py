"""
LDN GeoMedian/GeoMAD and Predicted LULC Mosaic Viewer
-----------------------
Uses TiTiler to visualise a MosaicJSON of either GeoMedian/GeoMAD or predicted LULC.
Can visualise single or multiple bands.
Tiles from separate per-band COGs using TiTiler + STACReader.
"""

import logging
import os
import re
import sys
from typing import Annotated, Literal

import httpx
from cogeo_mosaic.backends import MosaicBackend
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from mangum import Mangum
from rio_tiler.colormap import cmap as default_cmap
from rio_tiler.io import STACReader
from titiler.core.dependencies import AssetsExprParams, create_colormap_dependency
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.mosaic.factory import MosaicTilerFactory

GEOMAD_VERSION = "0-2-1-test"
PREDICTION_VERSION = "0-0-4-test"

GEOMAD_MOSAIC_BASE = f"https://source.coop/auspatious/geomad-sids/ls_geomad/{GEOMAD_VERSION}/mosaics"
PREDICTION_MOSAIC_BASE = f"https://source.coop/auspatious/lulc-sids/ls_lulc_prediction/{PREDICTION_VERSION}/mosaics"

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.WARNING,  # Package logging level.
    format="%(asctime)s | %(levelname)s | %(name)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
    force=True,
)
logger.setLevel(logging.INFO)  # Our logging level.


cmap = default_cmap.register(
    {
        "lulc": {
            255: (255, 255, 255, 0),  # No data   — transparent
            1: (0, 100, 0, 255),  # Tree Cover — dark green
            2: (255, 255, 76, 255),  # Grassland  — yellow
            3: (240, 150, 255, 255),  # Cropland   — pink
            4: (0, 150, 160, 255),  # Wetland    — teal
            5: (250, 0, 0, 255),  # Built-up   — red
            6: (0, 100, 200, 255),  # Water      — blue
            7: (180, 180, 180, 255),  # Other      — grey
        }
    }
)
ColorMapParams = create_colormap_dependency(cmap)

# Discover available mosaics by fetching the directory listing over HTTPS.
# Expects filenames like geomad_2020_mosaic.json or prediction_2020_mosaic.json.

MOSAIC_PATHS_GEOMAD: dict[str, str] = {}
MOSAIC_PATHS_PREDICTION: dict[str, str] = {}

MOSAIC_PATTERN = re.compile(r"_(\d{4})_mosaic\.json$")


# TODO: this should use boto3 s3.list_objects_v2 for non-source.coop buckets.
def _discover_mosaics(base_url: str) -> dict[str, str]:
    """Fetch an HTTPS directory listing and return {year: full_url} for each mosaic JSON found."""
    paths: dict[str, str] = {}
    try:
        response = httpx.get(base_url, follow_redirects=True, timeout=10)
        response.raise_for_status()
        # Parse hrefs from the HTML directory listing
        for href in re.findall(r'href="([^"]+_mosaic\.json)"', response.text):
            filename = href.split("/")[-1]
            match = MOSAIC_PATTERN.search(filename)
            if match:
                year = match.group(1)
                # Resolve relative or absolute hrefs
                url = href if href.startswith("http") else f"{base_url}/{filename}"
                paths[year] = url
    except Exception as e:
        logger.error(f"Failed to discover mosaics from {base_url}: {e}")
    return paths


try:
    MOSAIC_PATHS_GEOMAD = _discover_mosaics(GEOMAD_MOSAIC_BASE)
    MOSAIC_PATHS_PREDICTION = _discover_mosaics(PREDICTION_MOSAIC_BASE)
except Exception as e:
    logger.error(f"Failed to discover mosaics: {e}")

if not MOSAIC_PATHS_GEOMAD and not MOSAIC_PATHS_PREDICTION:
    raise RuntimeError(
        "Cannot start: failed to discover any mosaics from source.coop. "
        "Check network connectivity and that the base URLs are correct."
    )

logger.info(f"GeoMAD mosaics: {sorted(MOSAIC_PATHS_GEOMAD.keys())}")
logger.info(f"Prediction mosaics: {sorted(MOSAIC_PATHS_PREDICTION.keys())}")

DATASETS: dict[str, dict[str, str]] = {
    "geomad": MOSAIC_PATHS_GEOMAD,
    "prediction": MOSAIC_PATHS_PREDICTION,
}


# Custom path dependency


def mosaic_path_params(
    year: Annotated[
        str,
        Query(description="Year (e.g. '2020')", pattern=r"^\d{4}$"),
    ],
    dataset: Annotated[
        Literal["geomad", "prediction"],
        Query(description="Dataset name"),
    ],
) -> str:
    """Resolve dataset and year query parameters to a mosaic.json URL."""
    mosaic_paths = DATASETS.get(dataset)
    if mosaic_paths is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown dataset '{dataset}'. Valid options: {list(DATASETS.keys())}.",
        )
    if year in mosaic_paths:
        return str(mosaic_paths[year])
    raise HTTPException(
        status_code=404,
        detail=f"No mosaic found for year '{year}' in dataset '{dataset}'. "
        f"Available years: {sorted(mosaic_paths.keys())}.",
    )


# FastAPI app
app = FastAPI(
    title="LDN LULC Mosaic Viewer",
    description=(
        "Mosaic viewer for Landsat GeoMedian/GeoMAD and LULC Prediction data. "
        "Pass `dataset` (e.g. `dataset=geomad` or `dataset=prediction`), `year` (e.g. `year=2020`), and band assets as "
        "`assets=red&assets=green&assets=blue` or `assets=classification`."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_cache_control(request, call_next):
    """Add Cache-Control headers to tile responses for browser caching."""
    response = await call_next(request)
    if "/tiles/" in request.url.path and response.status_code == 200:
        # browsers cache tiles for 24 hours
        # CDN/proxy caches (e.g. CloudFront) cache for 7 days
        response.headers["Cache-Control"] = "public, max-age=86400, s-maxage=604800"
    return response


mosaic_factory = MosaicTilerFactory(
    backend=MosaicBackend,  # type: ignore
    dataset_reader=STACReader,
    path_dependency=mosaic_path_params,
    layer_dependency=AssetsExprParams,
    router_prefix="/mosaic",
    colormap_dependency=ColorMapParams,
)
app.include_router(mosaic_factory.router, prefix="/mosaic", tags=["Mosaic"])

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)


@app.get("/health", tags=["Health"])
def health():
    """Health check endpoint for load balancers and monitoring."""
    return {"status": "ok"}


@app.get("/config.json", tags=["Viewer"])
def config():
    """Return dynamic configuration for the frontend."""
    years_geomad = sorted(MOSAIC_PATHS_GEOMAD.keys())
    years_prediction = sorted(MOSAIC_PATHS_PREDICTION.keys())
    all_years = sorted(set(years_geomad + years_prediction))
    default_year = all_years[-1] if all_years else "2020"
    return {
        "years_geomad": years_geomad,
        "years_prediction": years_prediction,
        "all_years": all_years,
        "default_year": default_year,
        "geomad_version": GEOMAD_VERSION,
        "prediction_version": PREDICTION_VERSION,
    }


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/", tags=["Viewer"])
def root():
    """Serve the single-page map viewer."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), media_type="text/html")


# Lifespan "off" disables startup/shutdown events which can slow down Lambda cold starts.
handler = Mangum(app, lifespan="off")
