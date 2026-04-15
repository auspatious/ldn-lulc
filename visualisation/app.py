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

# from ldn.utils import GEOMAD_VERSION, PREDICTION_VERSION # Can't import these in app (ldn is not available in deployment).
# TODO: Pass these as a parameter in deploy.sh?
GEOMAD_VERSION = "0-0-4a"
PREDICTION_VERSION = "0-0-3"

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
            255: (255, 255, 255, 0),  # No data    — transparent
            1: (0, 100, 0, 255),  # Tree Cover — darkgreen
            2: (50, 205, 50, 255),  # Grassland  — limegreen
            3: (0, 255, 0, 255),  # Cropland   — lime
            4: (64, 224, 208, 255),  # Wetland    — turquoise
            5: (128, 128, 128, 255),  # Built-up   — gray
            6: (0, 0, 255, 255),  # Water      — blue
            7: (255, 255, 0, 255),  # Other      — yellow
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
s3 = boto3.client("s3")
MOSAIC_PATTERN = re.compile(r"(\w+)_(\d{4})_mosaic\.json$")

for prefix, dataset_prefix, version, paths_dict in [
    ("geomad", GEOMAD_DATASET_PREFIX, GEOMAD_VERSION, MOSAIC_PATHS_GEOMAD),
    (
        "prediction",
        PREDICTION_DATASET_PREFIX,
        PREDICTION_VERSION,
        MOSAIC_PATHS_PREDICTION,
    ),
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
        raise HTTPException(
            status_code=404,
            detail=f"Unknown dataset '{dataset}'. Valid options: {[d[0] for d in datasets]}.",
        )

    mocasic_paths = dataset_set[1]
    if year in mocasic_paths:
        return str(mocasic_paths[year])
    else:
        raise HTTPException(
            status_code=404,
            detail=f"No mosaic found for year '{year}' in dataset '{dataset}'. Available years: {sorted(mocasic_paths.keys())}.",
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
    backend=MosaicBackend,  # type: ignore
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

    html = f"""<!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8"/>
        <title>LDN LULC Mosaic Viewer</title>
        <style>
          body {{ font-family: monospace; max-width: 70vh; margin: 60px auto; padding: 0 20px; }}
          h1 {{ text-align: center; }}
          a {{ margin-right: .75rem; }}
          .item {{ margin-top: 2rem;  margin-bottom: 2rem; border-bottom: 2px solid #e7e7e7; padding-bottom: 1rem; }}
          .logo {{ height: 160px; margin: auto; display: block; margin-bottom: 1rem; }}
          select {{ font-family: monospace; font-size: 1rem; padding: 0.4rem 0.6rem; }}
          .selectors {{ display: flex; gap: 1rem; align-items: center; flex-wrap: wrap; }}
        </style>
      </head>
      <body>
        <a href="https://auspatious.com/" target="_blank" rel="noopener noreferrer"><img class="logo" src="https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/as-logo-horz-tag-colour.svg" alt="Auspatious logo"/></a>

        <div class="item">
          <h1>LDN LULC Mosaic Viewer</h1>
        </div>

        <div class="item">
          <div class="selectors">
            <select id="dataset-select">
              <option value="" disabled selected>Select dataset</option>
              <option value="geomedian">GeoMedian (RGB) v{GEOMAD_VERSION}</option>
              <option value="geomad">GeoMAD (S, E, BC) v{GEOMAD_VERSION}</option>
              <option value="classification">Classification v{PREDICTION_VERSION}</option>
              <option value="classification_unfiltered">Classification (unfiltered) v{PREDICTION_VERSION}</option>
              <option value="classification_probability">Classification (probability) v{PREDICTION_VERSION}</option>
            </select>
            <select id="year-select" disabled>
              <option value="" disabled selected>Select year</option>
            </select>
          </div>
          <script>
            var yearsByDataset = {{
              geomedian: {list(years_geomad)},
              geomad: {list(years_geomad)},
              classification: {list(years_prediction)},
              classification_unfiltered: {list(years_prediction)},
              classification_probability: {list(years_prediction)},
            }};
            var datasetSelect = document.getElementById("dataset-select");
            var yearSelect = document.getElementById("year-select");

            datasetSelect.addEventListener("change", function() {{
              var years = yearsByDataset[datasetSelect.value] || [];
              yearSelect.innerHTML = '<option value="" disabled selected>Select year</option>';
              years.forEach(function(y) {{
                var opt = document.createElement("option");
                opt.value = y; opt.textContent = y;
                yearSelect.appendChild(opt);
              }});
              yearSelect.disabled = years.length === 0;
            }});

            yearSelect.addEventListener("change", function() {{
              var ds = datasetSelect.value;
              var yr = yearSelect.value;
              if (!ds || !yr) return;
              if (ds === "geomedian") {{
                window.location.href = "/map?dataset=geomad&year=" + yr +
                  "&assets=red&assets=green&assets=blue&rescale=7200,12000&rescale=7200,12000&rescale=7200,12000";
              }} else if (ds === "geomad") {{
                window.location.href = "/map?dataset=geomad&year=" + yr +
                  "&assets=smad&assets=emad&assets=bcmad&rescale=0,0.0012&rescale=262,2150&rescale=0.006,0.04";
              }} else if (ds === "classification") {{
                window.location.href = "/map?dataset=prediction&year=" + yr +
                  "&assets=classification";
              }} else if (ds === "classification_unfiltered") {{
                window.location.href = "/map?dataset=prediction&year=" + yr +
                  "&assets=classification_unfiltered";
              }} else if (ds === "classification_probability") {{
                window.location.href = "/map?dataset=prediction&year=" + yr +
                  "&assets=classification_probability&colormap_name=rdylgn&rescale=0,100";
              }}
            }});
          </script>
        </div>

        <div class="item">
          <p class="docs"><a href="/docs">API docs</a></p>
        </div>
      </body>
      </html>"""

    return HTMLResponse(content=html)


@app.get("/map", tags=["Viewer"])
def map_viewer(
    dataset: Literal["geomad", "prediction"] = Query(...),
    year: str = Query(..., pattern=r"^\d{4}$"),
    assets: list[str] = Query(...),
    rescale: Optional[list[str]] = Query(None),
    colormap_name: Optional[str] = Query(None),
):
    """Render a full-page map viewer for the given dataset, year, and assets."""
    LULC_LEGEND = [
        (1, "rgb(0,100,0)", "Tree cover"),
        (2, "rgb(255,255,76)", "Grassland"),
        (3, "rgb(240,150,255)", "Cropland"),
        (4, "rgb(0,150,160)", "Wetland"),
        (5, "rgb(250,0,0)", "Built-up"),
        (6, "rgb(0,100,200)", "Water"),
        (7, "rgb(180,180,180)", "Other"),
    ]

    # Build the tile URL from the incoming query parameters.
    assets_qs = "&".join(f"assets={a}" for a in assets)
    tile_url = (
        f"/mosaic/WebMercatorQuad/map.html?dataset={dataset}&year={year}&{assets_qs}"
    )

    if rescale:
        tile_url += "&" + "&".join(f"rescale={r}" for r in rescale)
    if colormap_name:
        tile_url += f"&colormap_name={colormap_name}"

    # Show the LULC legend only for classification assets.
    classification_assets = {"classification", "classification_unfiltered"}
    if dataset == "prediction" and classification_assets.intersection(assets):
        if not colormap_name:
            tile_url += "&colormap_name=lulc"
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
    elif dataset == "prediction" and "classification_probability" in assets:
        legend_html = """
        <div id="legend">
          <div class="legend-title">Probability</div>
          <div style="display:flex;align-items:stretch;gap:6px;">
            <div style="width:18px;height:120px;background:linear-gradient(to bottom,#1a9641,#a6d96a,#ffffbf,#fdae61,#d7191c);border:1px solid #ccc;border-radius:3px;"></div>
            <div style="display:flex;flex-direction:column;justify-content:space-between;font-size:11px;">
              <span>100</span>
              <span>75</span>
              <span>50</span>
              <span>25</span>
              <span>0</span>
            </div>
          </div>
        </div>"""
    else:
        legend_html = ""

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
