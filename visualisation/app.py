"""
LDN GeoMedian/GeoMAD and Predicted LULC Mosaic Viewer
-----------------------
Uses TiTiler to visualise a MosaicJSON of either GeoMedian/GeoMAD or predicted LULC. Can visualise single or multiple bands.
Tiles from separate per-band COGs using TiTiler + STACReader.
"""

import logging
import os
import re
import sys
from typing import Annotated, Literal

import boto3
from cogeo_mosaic.backends import MosaicBackend
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from rio_tiler.io import STACReader
from rio_tiler.colormap import cmap as default_cmap
from titiler.core.dependencies import create_colormap_dependency
from titiler.core.dependencies import AssetsExprParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.mosaic.factory import MosaicTilerFactory
from mangum import Mangum

GEOMAD_VERSION = os.environ.get("GEOMAD_VERSION")
PREDICTION_VERSION = os.environ.get("PREDICTION_VERSION")

if not GEOMAD_VERSION or not PREDICTION_VERSION:
    raise ValueError(
        "GEOMAD_VERSION and PREDICTION_VERSION environment variables must be set (e.g. to '0-0-4a' and '0-0-3')."
    )

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

MOSAIC_S3_BUCKET = "data.ldn.auspatious.com"
GEOMAD_DATASET_PREFIX = "ausp_ls_geomad"
PREDICTION_DATASET_PREFIX = "ausp_ls_lulc_prediction"
MOSAIC_PATHS_GEOMAD: dict[str, str] = {}
MOSAIC_PATHS_PREDICTION: dict[str, str] = {}

# Scan S3 for mosaic JSONs on startup and populate paths dicts.
# Expects filenames like geomad_2020_mosaic.json or prediction_2020_mosaic.json.
MOSAIC_PATTERN = re.compile(r"(\w+)_(\d{4})_mosaic\.json$")

try:
    s3 = boto3.client("s3")
    for dataset_prefix, version, paths_dict in [
        (GEOMAD_DATASET_PREFIX, GEOMAD_VERSION, MOSAIC_PATHS_GEOMAD),
        (
            PREDICTION_DATASET_PREFIX,
            PREDICTION_VERSION,
            MOSAIC_PATHS_PREDICTION,
        ),
    ]:
        s3_prefix = f"{dataset_prefix}/{version}/mosaics/"
        # Capped at 1000 items (no pagination). Fine because there is one mosaic per year.
        response = s3.list_objects_v2(Bucket=MOSAIC_S3_BUCKET, Prefix=s3_prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            match = MOSAIC_PATTERN.search(key)
            if match:
                year = match.group(2)
                paths_dict[year] = f"s3://{MOSAIC_S3_BUCKET}/{key}"
except Exception as e:
    logger.error(f"Failed to scan S3 for mosaics: {e}")
    if not MOSAIC_PATHS_GEOMAD and not MOSAIC_PATHS_PREDICTION:
        raise RuntimeError(
            f"Cannot start: failed to discover any mosaics from s3://{MOSAIC_S3_BUCKET}. "
            f"Check AWS credentials and network connectivity. Error: {e}"
        ) from e

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
        Query(description="Dataset name (must be either 'geomad' or 'prediction')"),
    ],
) -> str:
    """Resolve dataset and year query parameters to a mosaic.json file path."""
    mosaic_paths = DATASETS.get(dataset)
    if mosaic_paths is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown dataset '{dataset}'. Valid options: {list(DATASETS.keys())}.",
        )

    if year in mosaic_paths:
        return str(mosaic_paths[year])
    else:
        raise HTTPException(
            status_code=404,
            detail=f"No mosaic found for year '{year}' in dataset '{dataset}'. Available years: {sorted(mosaic_paths.keys())}.",
        )


# FastAPI app
app = FastAPI(
    title="LDN LULC Mosaic Viewer",
    description=(
        "Mosaic viewer for Landsat GeoMedian/GeoMAD and LULC Prediction data. "
        "Pass a dataset as `dataset` (e.g. `dataset=geomad` or `dataset=prediction`) and year as `year` (e.g. `year=2020`) and band assets as "
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
    }


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/", tags=["Viewer"])
def root():
    """Serve the single-page map viewer."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), media_type="text/html")


handler = Mangum(
    app, lifespan="off"
)  # Lifespan "off" disables startup/shutdown events which can slow down Lambda cold starts.
