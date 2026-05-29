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

# Configuration from environment variables (set by Terraform/deploy).
# Buckets
PACIFIC_BUCKET = os.environ["PACIFIC_BUCKET"]
NON_PACIFIC_BUCKET = os.environ["NON_PACIFIC_BUCKET"]

# Owners (short prefixes used in S3 path construction)
PACIFIC_OWNER = os.environ["PACIFIC_OWNER"]
NON_PACIFIC_OWNER = os.environ["NON_PACIFIC_OWNER"]

# Versions
GEOMAD_VERSION = os.environ["GEOMAD_VERSION"]
PREDICTION_VERSION = os.environ["PREDICTION_VERSION"]

# Sensor and dataset IDs
SENSOR = os.environ["SENSOR"]
GEOMAD_DATASET_ID = os.environ["GEOMAD_DATASET_ID"]
PREDICTION_DATASET_ID = os.environ["PREDICTION_DATASET_ID"]

# Derived dataset prefixes: {owner}_{sensor}_{dataset_id}
PACIFIC_GEOMAD_PREFIX = f"{PACIFIC_OWNER}_{SENSOR}_{GEOMAD_DATASET_ID}"
PACIFIC_PREDICTION_PREFIX = f"{PACIFIC_OWNER}_{SENSOR}_{PREDICTION_DATASET_ID}"
NON_PACIFIC_GEOMAD_PREFIX = f"{NON_PACIFIC_OWNER}_{SENSOR}_{GEOMAD_DATASET_ID}"
NON_PACIFIC_PREDICTION_PREFIX = f"{NON_PACIFIC_OWNER}_{SENSOR}_{PREDICTION_DATASET_ID}"

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

MOSAIC_PATHS_GEOMAD_PACIFIC: dict[str, str] = {}
MOSAIC_PATHS_GEOMAD_NON_PACIFIC: dict[str, str] = {}
MOSAIC_PATHS_PREDICTION_PACIFIC: dict[str, str] = {}
MOSAIC_PATHS_PREDICTION_NON_PACIFIC: dict[str, str] = {}

# Scan S3 for mosaic JSONs on startup and populate paths dicts.
# Expects filenames like geomad_2020_mosaic.json or prediction_2020_mosaic.json.
MOSAIC_PATTERN = re.compile(r"(\w+)_(\d{4})_mosaic\.json$")

try:
    s3 = boto3.client("s3")
    for bucket, dataset_prefix, version, paths_dict in [
        (
            PACIFIC_BUCKET,
            PACIFIC_GEOMAD_PREFIX,
            GEOMAD_VERSION,
            MOSAIC_PATHS_GEOMAD_PACIFIC,
        ),
        (
            NON_PACIFIC_BUCKET,
            NON_PACIFIC_GEOMAD_PREFIX,
            GEOMAD_VERSION,
            MOSAIC_PATHS_GEOMAD_NON_PACIFIC,
        ),
        (
            PACIFIC_BUCKET,
            PACIFIC_PREDICTION_PREFIX,
            PREDICTION_VERSION,
            MOSAIC_PATHS_PREDICTION_PACIFIC,
        ),
        (
            NON_PACIFIC_BUCKET,
            NON_PACIFIC_PREDICTION_PREFIX,
            PREDICTION_VERSION,
            MOSAIC_PATHS_PREDICTION_NON_PACIFIC,
        ),
    ]:
        s3_prefix = f"{dataset_prefix}/{version}/mosaics/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            match = MOSAIC_PATTERN.search(key)
            if match:
                year = match.group(2)
                paths_dict[year] = f"s3://{bucket}/{key}"
except Exception as e:
    logger.error(f"Failed to scan S3 for mosaics: {e}")
    if not MOSAIC_PATHS_GEOMAD_PACIFIC and not MOSAIC_PATHS_GEOMAD_NON_PACIFIC:
        raise RuntimeError(
            f"Cannot start: failed to discover any mosaics. "
            f"Check AWS credentials and network connectivity. Error: {e}"
        ) from e

logger.info(f"GeoMAD pacific mosaics: {sorted(MOSAIC_PATHS_GEOMAD_PACIFIC.keys())}")
logger.info(
    f"GeoMAD non-pacific mosaics: {sorted(MOSAIC_PATHS_GEOMAD_NON_PACIFIC.keys())}"
)
logger.info(
    f"Prediction pacific mosaics: {sorted(MOSAIC_PATHS_PREDICTION_PACIFIC.keys())}"
)
logger.info(
    f"Prediction non-pacific mosaics: {sorted(MOSAIC_PATHS_PREDICTION_NON_PACIFIC.keys())}"
)

DATASETS: dict[str, dict[str, str]] = {
    "geomad_pacific": MOSAIC_PATHS_GEOMAD_PACIFIC,
    "geomad_non_pacific": MOSAIC_PATHS_GEOMAD_NON_PACIFIC,
    "prediction_pacific": MOSAIC_PATHS_PREDICTION_PACIFIC,
    "prediction_non_pacific": MOSAIC_PATHS_PREDICTION_NON_PACIFIC,
}


# Custom path dependency
def mosaic_path_params(
    year: Annotated[
        str,
        Query(description="Year (e.g. '2020')", pattern=r"^\d{4}$"),
    ],
    dataset: Annotated[
        Literal[
            "geomad_pacific",
            "geomad_non_pacific",
            "prediction_pacific",
            "prediction_non_pacific",
        ],
        Query(description="Dataset name"),
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
    years_geomad_pacific = sorted(MOSAIC_PATHS_GEOMAD_PACIFIC.keys())
    years_geomad_non_pacific = sorted(MOSAIC_PATHS_GEOMAD_NON_PACIFIC.keys())
    years_prediction_pacific = sorted(MOSAIC_PATHS_PREDICTION_PACIFIC.keys())
    years_prediction_non_pacific = sorted(MOSAIC_PATHS_PREDICTION_NON_PACIFIC.keys())
    all_years = sorted(
        set(
            years_geomad_pacific
            + years_geomad_non_pacific
            + years_prediction_pacific
            + years_prediction_non_pacific
        )
    )
    default_year = all_years[-1] if all_years else "2020"
    return {
        "years_geomad_pacific": years_geomad_pacific,
        "years_geomad_non_pacific": years_geomad_non_pacific,
        "years_prediction_pacific": years_prediction_pacific,
        "years_prediction_non_pacific": years_prediction_non_pacific,
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


handler = Mangum(
    app, lifespan="off"
)  # Lifespan "off" disables startup/shutdown events which can slow down Lambda cold starts.
