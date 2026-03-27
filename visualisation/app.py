"""
LDN GeoMedian/GeoMAD and Predicted LULC Mosaic Viewer
-----------------------
Uses TiTiler to visualise a MosaicJSON of either GeoMedian/GeoMAD or predicted LULC. Can visualise single or multiple bands.
Tiles from separate per-band COGs using TiTiler + STACReader.

Run:
    cd visualisation
    poetry run uvicorn app:app --host 0.0.0.0 --port 8081 --reload
"""


import logging
import os
import re
from typing import Annotated, Literal, Optional

import boto3
from cogeo_mosaic.backends import MosaicBackend
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from rio_tiler.io import STACReader
from rio_tiler.colormap import cmap as default_cmap
from titiler.core.dependencies import create_colormap_dependency
from titiler.core.dependencies import AssetsExprParams
from titiler.core.errors import DEFAULT_STATUS_CODES, add_exception_handlers
from titiler.mosaic.errors import MOSAIC_STATUS_CODES
from titiler.mosaic.factory import MosaicTilerFactory
from mangum import Mangum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


cmap = default_cmap.register({
    "lulc": {
        0: (255, 255, 255, 0),    # No data    — transparent
        1: (0,   100, 0,   255),  # Tree Cover — darkgreen
        2: (50,  205, 50,  255),  # Grassland  — limegreen
        3: (0,   255, 0,   255),  # Cropland   — lime
        4: (64,  224, 208, 255),  # Wetland    — turquoise
        5: (128, 128, 128, 255),  # Built-up   — gray
        6: (0,   0,   255, 255),  # Water      — blue
        7: (255, 255, 0,   255),  # Other      — yellow
    }
})
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
GEOMAD_DATASET_VERSION = "0-0-2"
PREDICTION_DATASET_PREFIX = "ausp_ls_prediction"
PREDICTION_DATASET_VERSION = "0-0-1"
MOSAIC_PATHS_GEOMAD: dict[str, str] = {}
MOSAIC_PATHS_PREDICTION: dict[str, str] = {}

# Scan S3 for mosaic JSONs on startup and populate paths dicts.
# Expects filenames like geomad_2020_mosaic.json or prediction_2020_mosaic.json.
s3 = boto3.client("s3")
MOSAIC_PATTERN = re.compile(r"(\w+)_(\d{4})_mosaic\.json$")

for prefix, dataset_prefix, version, paths_dict in [
    ("geomad", GEOMAD_DATASET_PREFIX, GEOMAD_DATASET_VERSION, MOSAIC_PATHS_GEOMAD),
    ("prediction", PREDICTION_DATASET_PREFIX, PREDICTION_DATASET_VERSION, MOSAIC_PATHS_PREDICTION),
]:
    s3_prefix = f"{dataset_prefix}/{version}/mosaics/"
    response = s3.list_objects_v2(Bucket=MOSAIC_S3_BUCKET, Prefix=s3_prefix)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        match = MOSAIC_PATTERN.search(key)
        if match:
            year = match.group(2)
            paths_dict[year] = f"s3://{MOSAIC_S3_BUCKET}/{key}"

logger.info(f"GeoMAD mosaics: {sorted(MOSAIC_PATHS_GEOMAD.keys())}")
logger.info(f"Prediction mosaics: {sorted(MOSAIC_PATHS_PREDICTION.keys())}")

datasets = [
    ("geomad", MOSAIC_PATHS_GEOMAD),
    ("prediction", MOSAIC_PATHS_PREDICTION),
]

# Custom path dependency
def MosaicPathParams(
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
    dataset_set = next((d for d in datasets if d[0] == dataset), None)
    if not dataset_set:
        raise HTTPException(status_code=404, detail=f"Unknown dataset '{dataset}'. Valid options: {[d[0] for d in datasets]}.")
    
    mocasic_paths = dataset_set[1]
    if year in mocasic_paths:
        return str(mocasic_paths[year])
    else:
        raise HTTPException(status_code=404, detail=f"No mosaic found for year '{year}' in dataset '{dataset}'. Available years: {sorted(mocasic_paths.keys())}.")


# FastAPI app
app = FastAPI(
    title="LDN LULC Mosaic Viewer",
    description=(
        "Mosaic viewer for Landsat GeoMedian/GeoMAD and LULC Prediction data. "
        "Pass a dataset as `dataset` (e.g. `dataset=geomad` or `dataset=prediction`) and year as `year` (e.g. `year=2020`) and band assets as "
        "`assets=red&assets=green&assets=blue` or `assets=lulc`."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    colormap_dependency=ColorMapParams,
)
app.include_router(mosaic_factory.router, prefix="/mosaic", tags=["Mosaic"])

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)


@app.get("/", tags=["Info"])
def root():
    years_geomad = sorted(MOSAIC_PATHS_GEOMAD.keys())
    years_prediction = sorted(MOSAIC_PATHS_PREDICTION.keys())

    def geomad_link(y):
        return f'<a href="/map?dataset=geomad&year={y}&assets=red&assets=green&assets=blue&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500">{y}</a>'

    def prediction_link(y):
        return f'<a href="/map?dataset=prediction&year={y}&assets=lulc&colormap_name=lulc">{y}</a>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>LDN LULC Mosaic Viewer</title>
  <style>
    body {{ font-family: monospace; max-width: 600px; margin: 60px auto; padding: 0 20px; }}
    h1 {{ font-size: 1.2rem; margin-bottom: 2rem; }}
    h2 {{ font-size: .85rem; text-transform: uppercase; color: #999; margin: 1.5rem 0 .5rem; }}
    a {{ color: #2563eb; text-decoration: none; margin-right: .75rem; }}
    a:hover {{ text-decoration: underline; }}
    .meta {{ margin-top: 3rem; font-size: .75rem; color: #bbb; }}
  </style>
</head>
<body>
  <h1>LDN LULC Mosaic Viewer</h1>

  <h2>GeoMAD</h2>
  {''.join(geomad_link(y) for y in years_geomad)}

  <h2>Prediction</h2>
  {''.join(prediction_link(y) for y in years_prediction)}

  <p class="meta"><a href="/docs">API docs</a></p>
</body>
</html>"""

    return HTMLResponse(content=html)


@app.get("/map", tags=["Viewer"])
def map_viewer(
    dataset: Literal["geomad", "prediction"] = Query(...),
    year: str = Query(..., pattern=r"^\d{4}$"),
    colormap_name: Optional[str] = Query(None),
):
    LULC_LEGEND = [
        (0, "rgba(255,255,255,0)",   "No data"),
        (1, "rgb(0,100,0)",          "Tree Cover"),
        (2, "rgb(50,205,50)",        "Grassland"),
        (3, "rgb(0,255,0)",          "Cropland"),
        (4, "rgb(64,224,208)",       "Wetland"),
        (5, "rgb(128,128,128)",      "Built-up"),
        (6, "rgb(0,0,255)",          "Water"),
        (7, "rgb(255,255,0)",        "Other"),
    ]

    if dataset == "geomad":
        tile_url = (
            f"/mosaic/WebMercatorQuad/map.html?dataset={dataset}&year={year}"
            "&assets=red&assets=green&assets=blue"
            "&rescale=7000,12500&rescale=7000,12500&rescale=7000,12500"
        )
        legend_html = ""
    elif dataset == "prediction":
        tile_url = (
            f"/mosaic/WebMercatorQuad/map.html?dataset={dataset}&year={year}"
            f"&assets=lulc&colormap_name={colormap_name or 'lulc'}"
        )
        legend_items = "".join(
            f"""<div class="legend-item">
                  <span class="swatch" style="background:{color};border:1px solid #ccc;"></span>
                  <span>{label}</span>
                </div>"""
            for _, color, label in LULC_LEGEND
        )
        legend_html = f"""
        <div id="legend">
          <div class="legend-title">Land Cover</div>
          {legend_items}
        </div>"""
    else:
        raise HTTPException(status_code=400, detail=f"Unknown dataset '{dataset}'. Valid options: 'geomad' or 'prediction'.")

    html = f"""<!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8"/>
      <title>LDN {dataset} {year}</title>
      <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        html, body {{ height: 100%; }}
        iframe {{ width: 100%; height: 100%; border: none; display: block; }}

        #legend {{
          position: fixed;
          bottom: 50px;
          right: 10px;
          z-index: 9999;
          background: #FFF;
          border: 2px solid rgba(0, 0, 0, 0.2);
          border-radius: 4px;
          padding: 12px 14px;
          font-family: monospace;
          font-size: 12px;
          color: #000;
          pointer-events: none;
          backdrop-filter: blur(4px);
        }}
        .legend-title {{
          font-weight: bold;
          margin-bottom: 8px;
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: .06em;
        }}
        .legend-item {{
          display: flex;
          align-items: center;
          gap: 8px;
          margin-bottom: 5px;
        }}
        .legend-item:last-child {{ margin-bottom: 0; }}
        .swatch {{
          width: 14px;
          height: 14px;
          border-radius: 3px;
          flex-shrink: 0;
        }}
      </style>
    </head>
    <body>
      <iframe src="{tile_url}"></iframe>
      {legend_html}
    </body>
    </html>"""

    return HTMLResponse(content=html)

handler = Mangum(app, lifespan="off")
