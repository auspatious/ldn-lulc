"""
LDN GeoMedian/GeoMAD and Predicted LULC Mosaic Viewer
-----------------------
Uses TiTiler to visualise a MosaicJSON of either GeoMedian/GeoMAD or predicted LULC.
Can visualise single or multiple bands.
Tiles from separate per-band COGs using TiTiler + STACReader.
"""

import json
import logging
import os
import re
import sys
import tempfile
import urllib.request
from typing import Annotated, Literal

import boto3
from botocore import UNSIGNED
from botocore.config import Config
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

GEOMAD_VERSION = "0-2-1"
LULC_VERSION = "0-0-9"

SOURCE_COOP_ENDPOINT = "https://data.source.coop"
SOURCE_COOP_ACCOUNT = "auspatious"

LULC_BUCKET = "dep-public-staging"
LULC_REGION = "us-west-2"

# LULC is published per-region. Each region's mosaics live under their own
# prefix but share a version; for years where more than one region has a
# mosaic, we merge them into a single combined MosaicJSON so the frontend can
# keep treating "lulc" as one seamless dataset.
LULC_REGIONAL_PREFIXES = {
    "pacific": "dep_ls_lulc",
    "non-pacific": "ci_ls_lulc",
}


# GDAL/rasterio (used by rio-tiler/STACReader to actually read COG pixels) has its
# own credential resolution, separate from the boto3 client above used just for
# listing. Without this, GDAL tries instance-profile creds via the EC2 metadata
# service (169.254.169.254), which doesn't exist off-AWS and hangs until timeout.
os.environ.setdefault("AWS_NO_SIGN_REQUEST", "YES")
os.environ.setdefault("AWS_REGION", LULC_REGION)
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")

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

# Discover available mosaics by listing the backing bucket.
# Expects keys/filenames like geomad_2020_mosaic.json or prediction_2020_mosaic.json.

MOSAIC_PATTERN = re.compile(r"(\d{4})_mosaic\.json$")


def _make_s3_client(endpoint_url: str | None = None) -> boto3.client:
    """Create an unsigned S3 client, optionally with a custom endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        config=Config(signature_version=UNSIGNED),
    )


def _discover_mosaics(bucket: str, prefix: str, url_for_key, endpoint_url: str | None = None) -> dict[str, str]:
    """
    List mosaics in an S3-compatible bucket. Returns {year: https_url}.
    `url_for_key` builds the public HTTPS URL for a given object key.
    """
    s3 = _make_s3_client(endpoint_url=endpoint_url)
    paths: dict[str, str] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = MOSAIC_PATTERN.search(key)
            if match:
                year = match.group(1)
                paths[year] = url_for_key(key)
                logger.info(f"Discovered mosaic: {year} → {paths[year]}")
    return paths


# TODO: Remove this kinda hack once LULC is created for both regions together.
def _merge_mosaicjson_docs(docs: list[dict]) -> dict:
    """Merge multiple MosaicJSON documents (same year, different regions) into one.

    Assumes all docs share the same minzoom/maxzoom (true for mosaics produced
    by the same pipeline/version), so quadkeys are directly comparable. Since
    the regions are geographically disjoint, quadkeys shouldn't collide, but
    if they do we just concatenate the asset lists.
    """
    base = dict(docs[0])
    combined_tiles: dict[str, list] = {}
    bounds = None
    for doc in docs:
        for quadkey, assets in doc.get("tiles", {}).items():
            combined_tiles.setdefault(quadkey, []).extend(assets)
        b = doc.get("bounds")
        if b:
            bounds = (
                b
                if bounds is None
                else [
                    min(bounds[0], b[0]),
                    min(bounds[1], b[1]),
                    max(bounds[2], b[2]),
                    max(bounds[3], b[3]),
                ]
            )
    base["tiles"] = combined_tiles
    if bounds:
        base["bounds"] = bounds
        base["center"] = [(bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2, base.get("minzoom", 0)]
    return base


def _merge_regional_mosaics(regional_paths: list[dict[str, str]], label: str) -> dict[str, str]:
    """
    Merge {year: url} dicts from multiple regional prefixes of the same
    logical dataset into a single {year: path} dict.

    Years present in only one region pass through unchanged (their original
    URL). Years present in 2+ regions are merged into a combined MosaicJSON
    written to /tmp and referenced by local file path — cogeo-mosaic opens
    plain local paths natively, no extra backend needed.
    """
    years: dict[str, list[str]] = {}
    for paths in regional_paths:
        for year, url in paths.items():
            years.setdefault(year, []).append(url)

    merged: dict[str, str] = {}
    for year, urls in years.items():
        if len(urls) == 1:
            merged[year] = urls[0]
            continue
        try:
            docs = []
            for url in urls:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    docs.append(json.loads(resp.read()))
            merged_doc = _merge_mosaicjson_docs(docs)
            out_path = os.path.join(tempfile.gettempdir(), f"merged_{label}_{year}_mosaic.json")
            with open(out_path, "w") as f:
                json.dump(merged_doc, f)
            merged[year] = out_path
            logger.info(
                f"Merged {len(urls)} regional mosaics for {label} {year} "
                f"({sum(len(d.get('tiles', {})) for d in docs)} tiles total) → {out_path}"
            )
        except Exception as e:
            logger.error(f"Failed to merge {label} mosaics for {year}: {e}; using first region only")
            merged[year] = urls[0]
    return merged


try:
    geomad_prefix = f"geomad-sids/ls_geomad/{GEOMAD_VERSION}/mosaics"
    MOSAIC_PATHS_GEOMAD = _discover_mosaics(
        bucket=SOURCE_COOP_ACCOUNT,
        prefix=geomad_prefix,
        url_for_key=lambda key: f"{SOURCE_COOP_ENDPOINT}/{SOURCE_COOP_ACCOUNT}/{key}",
        endpoint_url=SOURCE_COOP_ENDPOINT,
    )

    lulc_regions: dict[str, dict[str, str]] = {}
    for region_name, region_prefix in LULC_REGIONAL_PREFIXES.items():
        prefix = f"{region_prefix}/{LULC_VERSION}/mosaics"
        lulc_regions[region_name] = _discover_mosaics(
            bucket=LULC_BUCKET,
            prefix=prefix,
            url_for_key=lambda key: f"https://{LULC_BUCKET}.s3.{LULC_REGION}.amazonaws.com/{key}",
        )
        logger.info(f"LULC ({region_name}) mosaics: {sorted(lulc_regions[region_name].keys())}")

    MOSAIC_PATHS_LULC = _merge_regional_mosaics(list(lulc_regions.values()), label="lulc")
except Exception as e:
    logger.error(f"Failed to discover mosaics: {e}")
    MOSAIC_PATHS_GEOMAD = {}
    MOSAIC_PATHS_LULC = {}

if not MOSAIC_PATHS_GEOMAD and not MOSAIC_PATHS_LULC:
    raise RuntimeError(
        "Cannot start: failed to discover any mosaics from source.coop or "
        f"s3://{LULC_BUCKET}. Check network connectivity and bucket/prefix names."
    )

logger.info(f"GeoMAD mosaics: {sorted(MOSAIC_PATHS_GEOMAD.keys())}")
logger.info(f"LULC mosaics (merged, all regions): {sorted(MOSAIC_PATHS_LULC.keys())}")

DATASETS: dict[str, dict[str, str]] = {
    "geomad": MOSAIC_PATHS_GEOMAD,
    "lulc": MOSAIC_PATHS_LULC,
}


# Custom path dependency


def mosaic_path_params(
    year: Annotated[
        str,
        Query(description="Year (e.g. '2020')", pattern=r"^\d{4}$"),
    ],
    dataset: Annotated[
        Literal["geomad", "lulc"],
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
        "Mosaic viewer for Landsat GeoMedian/GeoMAD and LULC data. "
        "Pass `dataset` (e.g. `dataset=geomad` or `dataset=lulc`), `year` (e.g. `year=2020`), and band assets as "
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
    years_lulc = sorted(MOSAIC_PATHS_LULC.keys())
    all_years = sorted(set(years_geomad + years_lulc))
    default_year = all_years[-1] if all_years else "2020"
    return {
        "years_geomad": years_geomad,
        "years_lulc": years_lulc,
        "all_years": all_years,
        "default_year": default_year,
        "geomad_version": GEOMAD_VERSION,
        "lulc_version": LULC_VERSION,
    }


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/sids-tiles.geojson", tags=["Viewer"])
def sids_tiles():
    """Serve the SIDS tiles GeoJSON for map overlay."""
    return FileResponse(os.path.join(STATIC_DIR, "sids_all_tiles.geojson"), media_type="application/json")


@app.get("/", tags=["Viewer"])
def root():
    """Serve the single-page map viewer."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"), media_type="text/html")


# Lifespan "off" disables startup/shutdown events which can slow down Lambda cold starts.
handler = Mangum(app, lifespan="off")
