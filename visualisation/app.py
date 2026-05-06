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
from fastapi.responses import HTMLResponse
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
    # This doesn't have pagination so is capped at 1000 items. We won't hit this because it is one mosaic per year.
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

    mosaic_paths = dataset_set[1]
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
    path_dependency=MosaicPathParams,
    layer_dependency=AssetsExprParams,
    router_prefix="/mosaic",
    colormap_dependency=ColorMapParams,
)
app.include_router(mosaic_factory.router, prefix="/mosaic", tags=["Mosaic"])

add_exception_handlers(app, DEFAULT_STATUS_CODES)
add_exception_handlers(app, MOSAIC_STATUS_CODES)


@app.get("/", tags=["Viewer"])
def root():
    """Render the single-page map viewer with all layers, year selector, opacity, and swipe."""
    years_geomad = sorted(MOSAIC_PATHS_GEOMAD.keys())
    years_prediction = sorted(MOSAIC_PATHS_PREDICTION.keys())
    all_years = sorted(set(years_geomad + years_prediction))
    default_year = all_years[-1] if all_years else "2020"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>LDN LULC Viewer</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet-compare@1/dist/leaflet-compare.css"/>
  <style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    html, body {{ height:100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; }}
    #map {{ height:100%; width:100%; }}

    /* Control panel */
    #controls {{
      position:absolute; top:130px; left:10px; right:10px; z-index:1000;
      background:rgba(255,255,255,0.95); border-radius:8px;
      padding:14px 16px;
      box-shadow:0 2px 8px rgba(0,0,0,0.25);
      font-size:13px; max-height:calc(100vh - 60px); overflow-y:auto;
      transition: transform 0.3s ease;
      max-width:300px;
    }}
    #controls.collapsed {{ transform:translateX(calc(-100% - 20px)); }}
    #toggle-controls {{
      position:absolute; top:80px; left:10px; z-index:1001;
      background:rgba(255,255,255,0.95); border:none; border-radius:6px;
      width:36px; height:36px; font-size:18px; cursor:pointer;
      box-shadow:0 2px 6px rgba(0,0,0,0.25);
    }}
    #controls h3 {{ margin:0 0 10px; font-size:14px; }}
    #controls label {{ display:block; margin:4px 0; cursor:pointer; }}
    #controls select, #controls input[type=range] {{ width:100%; margin:2px 0 6px; }}
    .section {{ margin-bottom:12px; padding-bottom:10px; border-bottom:1px solid #e0e0e0; }}
    .section:last-child {{ border-bottom:none; margin-bottom:0; padding-bottom:0; }}
    .opacity-row {{ display:flex; align-items:center; gap:6px; margin:2px 0 4px; }}
    .opacity-row input {{ flex:1; }}
    .opacity-row span {{ min-width:32px; text-align:right; font-size:11px; color:#666; }}

    /* Legend */
    #legend-class, #legend-prob {{
      position:absolute; bottom:30px; right:10px; z-index:1000;
      background:rgba(255,255,255,0.95); border-radius:8px;
      padding:12px 14px; box-shadow:0 2px 8px rgba(0,0,0,0.25);
      font-size:12px;
    }}
    .legend-title {{ font-weight:bold; margin-bottom:8px; font-size:11px; text-transform:uppercase; letter-spacing:.06em; }}
    .legend-item {{ display:flex; align-items:center; gap:8px; margin-bottom:4px; }}
    .legend-item:last-child {{ margin-bottom:0; }}
    .swatch {{ width:14px; height:14px; border-radius:3px; border:1px solid #ccc; flex-shrink:0; }}

    /* Tooltip */
    #tooltip {{
      position:absolute; z-index:1001; pointer-events:none;
      background:rgba(0,0,0,0.8); color:#fff; border-radius:4px;
      padding:6px 10px; font-size:12px; font-family:monospace;
      white-space:pre-line; display:none;
    }}

    /* Loading indicator */
    #loading {{
      position:absolute; top:10px; right:10px; z-index:1001;
      background:rgba(0,0,0,0.7); color:#fff; border-radius:6px;
      padding:8px 14px; font-size:12px; display:none;
      align-items:center; gap:8px;
    }}
    #loading.active {{ display:flex; }}
    .spinner {{
      width:14px; height:14px; border:2px solid rgba(255,255,255,0.3);
      border-top-color:#fff; border-radius:50%;
      animation:spin 0.8s linear infinite;
    }}
    @keyframes spin {{ to {{ transform:rotate(360deg); }} }}

    /* Desktop */
    @media (min-width: 801px) {{
      #controls {{
        top:10px; left:55px; right:auto;
        min-width:260px;
        max-height:calc(100vh - 30px);
      }}
      #controls.collapsed {{ transform:translateX(calc(-100% - 60px)); }}
      #toggle-controls {{ display:none; }}
    }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div id="loading"><div class="spinner"></div><span>Loading tiles...</span></div>
  <button id="toggle-controls">&#9776;</button>

  <div id="controls">
    <div style="text-align:center;margin-bottom:10px;">
      <a href="https://auspatious.com/" target="_blank" rel="noopener noreferrer">
        <img src="https://s3.us-west-2.amazonaws.com/data.ldn.auspatious.com/as-logo-horz-tag-colour.svg"
             alt="Auspatious" style="width:100%;"/>
      </a>
    </div>

    <div class="section">
      <h3>Basemap</h3>
      <select id="basemap-select">
        <option value="hybrid" selected>Satellite (hybrid)</option>
        <option value="satellite">Satellite</option>
        <option value="osm">OpenStreetMap</option>
        <option value="light">Light</option>
        <option value="dark">Dark</option>
      </select>
    </div>

    <div class="section">
      <h3>Year</h3>
      <select id="year-select">
        {"".join(f'<option value="{y}"' + (" selected" if y == default_year else "") + f">{y}</option>" for y in all_years)}
      </select>
    </div>

    <div class="section">
      <h3>Layers</h3>
      <label><input type="checkbox" id="chk-class" checked/> Classification</label>
      <label><input type="checkbox" id="chk-rgb" checked/> GeoMedian (RGB)</label>
      <label><input type="checkbox" id="chk-geomad"/> GeoMAD (S, E, BC)</label>
      <label><input type="checkbox" id="chk-classuf"/> Classification (unfiltered)</label>
      <label><input type="checkbox" id="chk-prob"/> Probability</label>
    </div>

    <div class="section">
      <h3>Opacity</h3>
      <div id="opacity-sliders"></div>
    </div>

    <div class="section">
      <h3>Swipe compare</h3>
      <p style="font-size:11px;color:#666;margin-bottom:6px;">Select two different layers to compare side-by-side.</p>
      <label>Left: <select id="swipe-left"><option value="">None</option></select></label>
      <label>Right: <select id="swipe-right"><option value="">None</option></select></label>
      <p id="swipe-hint" style="font-size:11px;color:#c00;margin-top:4px;display:none;"></p>
    </div>

    <div class="section" style="text-align:center;">
      <a href="/docs" style="font-size:11px; color:#666;">API docs</a>
    </div>
  </div>

  <div id="legend-class" style="display:none;">
    <div class="legend-title">Land Cover</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(0,100,0);"></span>Tree cover</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(255,255,76);"></span>Grassland</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(240,150,255);"></span>Cropland</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(0,150,160);"></span>Wetland</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(250,0,0);"></span>Built-up</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(0,100,200);"></span>Water</div>
    <div class="legend-item"><span class="swatch" style="background:rgb(180,180,180);"></span>Other</div>
  </div>

  <div id="legend-prob" style="display:none;">
    <div class="legend-title">Probability</div>
    <div style="display:flex;align-items:stretch;gap:6px;">
      <div style="width:18px;height:120px;background:linear-gradient(to bottom,#1a9641,#a6d96a,#ffffbf,#fdae61,#d7191c);border:1px solid #ccc;border-radius:3px;"></div>
      <div style="display:flex;flex-direction:column;justify-content:space-between;font-size:11px;">
        <span>100%</span><span>75%</span><span>50%</span><span>25%</span><span>0%</span>
      </div>
    </div>
  </div>

  <div id="tooltip"></div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet-compare@1/dist/leaflet-compare.js"></script>
  <script>
    // Mobile toggle for controls panel
    document.getElementById("toggle-controls").addEventListener("click", function() {{
      var panel = document.getElementById("controls");
      panel.classList.toggle("collapsed");
      this.textContent = panel.classList.contains("collapsed") ? "\u2630" : "\u2715";
    }});

    var YEARS_GEOMAD = {list(years_geomad)};
    var YEARS_PREDICTION = {list(years_prediction)};
    var CLASS_LABELS = {{1:"Tree cover",2:"Grassland",3:"Cropland",4:"Wetland",5:"Built-up",6:"Water",7:"Other",255:"No data"}};

    var map = L.map("map", {{center:[0,160], zoom:3}});

    var BASEMAP_CONFIGS = {{
      hybrid: [
        {{ url:"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}", opts:{{ attribution:'&copy; Esri', maxZoom:19 }} }},
        {{ url:"https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{{z}}/{{y}}/{{x}}", opts:{{ maxZoom:19, pane:"overlayPane" }} }}
      ],
      satellite: [
        {{ url:"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}", opts:{{ attribution:'&copy; Esri', maxZoom:19 }} }}
      ],
      osm: [
        {{ url:"https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", opts:{{ attribution:'&copy; <a href="https://openstreetmap.org">OSM</a>', maxZoom:19 }} }}
      ],
      light: [
        {{ url:"https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}@2x.png", opts:{{ attribution:'&copy; <a href="https://carto.com/">CARTO</a>', maxZoom:19 }} }}
      ],
      dark: [
        {{ url:"https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}@2x.png", opts:{{ attribution:'&copy; <a href="https://carto.com/">CARTO</a>', maxZoom:19 }} }}
      ]
    }};

    var activeBasemapLayers = [];
    function setBasemap(name) {{
      activeBasemapLayers.forEach(function(l) {{ map.removeLayer(l); }});
      var configs = BASEMAP_CONFIGS[name] || BASEMAP_CONFIGS.hybrid;
      activeBasemapLayers = configs.map(function(cfg) {{
        var l = L.tileLayer(cfg.url, cfg.opts);
        l.addTo(map); l.bringToBack(); return l;
      }});
    }}
    setBasemap("hybrid");

    document.getElementById("basemap-select").addEventListener("change", function() {{
      setBasemap(this.value);
    }});

    var BASE = window.location.origin + "/mosaic/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?";

    // Layer definitions (order determines z-order on map: first = bottom)
    var LAYER_ORDER = ["prob", "classuf", "geomad", "rgb", "class"];
    var LAYERS = {{
      "class":    {{ label:"Classification", ds:"prediction", dsYears:YEARS_PREDICTION,
                     qs:"assets=classification&colormap_name=lulc" }},
      "rgb":      {{ label:"GeoMedian (RGB)", ds:"geomad", dsYears:YEARS_GEOMAD,
                     qs:"assets=red&assets=green&assets=blue&rescale=7200,12000&rescale=7200,12000&rescale=7200,12000" }},
      "geomad":   {{ label:"GeoMAD (S, E, BC)", ds:"geomad", dsYears:YEARS_GEOMAD,
                     qs:"assets=smad&assets=emad&assets=bcmad&rescale=0,0.0012&rescale=262,2150&rescale=0.006,0.04" }},
      "classuf":  {{ label:"Classification (unfiltered)", ds:"prediction", dsYears:YEARS_PREDICTION,
                     qs:"assets=classification_unfiltered&colormap_name=lulc" }},
      "prob":     {{ label:"Probability", ds:"prediction", dsYears:YEARS_PREDICTION,
                     qs:"assets=classification_probability&colormap_name=rdylgn&rescale=0,100" }},
    }};

    var tileLayers = {{}};
    var currentYear = "{default_year}";
    var swipeControl = null;

    function tileUrl(key, year) {{
      var L = LAYERS[key];
      return BASE + "dataset=" + L.ds + "&year=" + year + "&" + L.qs;
    }}

    var CHECKBOX_MAP = {{"class":"chk-class", rgb:"chk-rgb", geomad:"chk-geomad", classuf:"chk-classuf", prob:"chk-prob"}};

    function updateCheckboxAvailability() {{
      for (var key in CHECKBOX_MAP) {{
        var cb = document.getElementById(CHECKBOX_MAP[key]);
        var lbl = cb.parentElement;
        var available = LAYERS[key].dsYears.indexOf(currentYear) >= 0;
        cb.disabled = !available;
        lbl.style.opacity = available ? "1" : "0.4";
        lbl.style.cursor = available ? "pointer" : "default";
        if (!available && cb.checked) {{
          cb.checked = false;
        }}
      }}
    }}

    var loadingEl = document.getElementById("loading");
    var tilesLoading = 0;

    function updateLoadingIndicator() {{
      if (tilesLoading > 0) {{
        loadingEl.classList.add("active");
      }} else {{
        loadingEl.classList.remove("active");
      }}
    }}

    function createTileLayer(key, year) {{
      var layer = L.tileLayer(tileUrl(key, year), {{
        opacity: 0.75, maxZoom: 18, tileSize: 256,
        errorTileUrl: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
      }});
      layer.on("tileloadstart", function() {{ tilesLoading++; updateLoadingIndicator(); }});
      layer.on("tileload tileunload tileerror", function() {{ tilesLoading = Math.max(0, tilesLoading - 1); updateLoadingIndicator(); }});
      return layer;
    }}

    function rebuildLayers() {{
      // Remove swipe control before removing layers it references
      if (swipeControl) {{
        swipeControl.remove();
        swipeControl = null;
      }}

      // Remove existing tile layers from map
      for (var k in tileLayers) {{
        if (map.hasLayer(tileLayers[k])) map.removeLayer(tileLayers[k]);
      }}
      tileLayers = {{}};
      tilesLoading = 0;
      updateLoadingIndicator();

      updateCheckboxAvailability();

      // Create layers in z-order (first added = bottom)
      LAYER_ORDER.forEach(function(key) {{
        var cb = document.getElementById(CHECKBOX_MAP[key]);
        if (cb && cb.checked && LAYERS[key].dsYears.indexOf(currentYear) >= 0) {{
          tileLayers[key] = createTileLayer(key, currentYear);
          tileLayers[key].addTo(map);
        }}
      }});

      rebuildOpacitySliders();
      rebuildSwipeSelects();
      updateSwipe();
      updateLegends();
    }}

    function syncLayers() {{
      // Only add/remove layers that changed, preserving existing ones
      var desired = {{}};
      LAYER_ORDER.forEach(function(key) {{
        var cb = document.getElementById(CHECKBOX_MAP[key]);
        if (cb && cb.checked && LAYERS[key].dsYears.indexOf(currentYear) >= 0) {{
          desired[key] = true;
        }}
      }});

      // Remove layers no longer desired
      for (var k in tileLayers) {{
        if (!desired[k]) {{
          map.removeLayer(tileLayers[k]);
          delete tileLayers[k];
        }}
      }}

      // Add new layers (in z-order)
      LAYER_ORDER.forEach(function(key) {{
        if (desired[key] && !tileLayers[key]) {{
          tileLayers[key] = createTileLayer(key, currentYear);
          tileLayers[key].addTo(map);
        }}
      }});

      rebuildOpacitySliders();
      rebuildSwipeSelects();
      updateSwipe();
      updateLegends();
    }}

    function updateLegends() {{
      var hasClass = tileLayers["class"] || tileLayers["classuf"];
      var hasProb = tileLayers["prob"];
      document.getElementById("legend-class").style.display = hasClass ? "block" : "none";
      var probEl = document.getElementById("legend-prob");
      probEl.style.display = hasProb ? "block" : "none";
      // Stack probability legend above classification legend if both visible
      probEl.style.bottom = hasClass ? "210px" : "30px";
    }}

    function rebuildOpacitySliders() {{
      var container = document.getElementById("opacity-sliders");
      container.innerHTML = "";
      var displayOrder = LAYER_ORDER.slice().reverse();
      displayOrder.forEach(function(key) {{
        if (!tileLayers[key]) return;
        var row = document.createElement("div");
        row.className = "opacity-row";
        var lbl = document.createElement("span");
        lbl.textContent = LAYERS[key].label.split("(")[0].trim();
        lbl.style.fontSize = "11px";
        lbl.style.minWidth = "80px";
        var inp = document.createElement("input");
        inp.type = "range"; inp.min = "0"; inp.max = "1"; inp.step = "0.05";
        inp.value = String(tileLayers[key].options.opacity);
        var val = document.createElement("span");
        val.textContent = Math.round(inp.value * 100) + "%";
        inp.addEventListener("input", (function(k, v) {{
          return function(e) {{
            var op = parseFloat(e.target.value);
            tileLayers[k].setOpacity(op);
            v.textContent = Math.round(op * 100) + "%";
            pushState();
          }};
        }})(key, val));
        row.appendChild(lbl);
        row.appendChild(inp);
        row.appendChild(val);
        container.appendChild(row);
      }});
    }}

    function rebuildSwipeSelects() {{
      var displayOrder = LAYER_ORDER.slice().reverse();
      ["swipe-left","swipe-right"].forEach(function(id) {{
        var sel = document.getElementById(id);
        var prev = sel.value;
        sel.innerHTML = '<option value="">None</option>';
        var found = false;
        displayOrder.forEach(function(k) {{
          if (!tileLayers[k]) return;
          var opt = document.createElement("option");
          opt.value = k;
          opt.textContent = LAYERS[k].label;
          if (k === prev) {{ opt.selected = true; found = true; }}
          sel.appendChild(opt);
        }});
        if (!found) sel.value = "";
      }});
    }}

    function updateSwipe() {{
      var hint = document.getElementById("swipe-hint");
      hint.style.display = "none";
      hint.textContent = "";

      if (swipeControl) {{
        swipeControl.remove();
        swipeControl = null;
      }}

      var leftKey = document.getElementById("swipe-left").value;
      var rightKey = document.getElementById("swipe-right").value;

      if (!leftKey && !rightKey) return;
      if (leftKey && !rightKey) {{
        hint.textContent = "Select a right layer to compare.";
        hint.style.display = "block";
        return;
      }}
      if (!leftKey && rightKey) {{
        hint.textContent = "Select a left layer to compare.";
        hint.style.display = "block";
        return;
      }}
      if (leftKey === rightKey) {{
        hint.textContent = "Left and right must be different layers.";
        hint.style.display = "block";
        return;
      }}
      if (!tileLayers[leftKey] || !tileLayers[rightKey]) return;

      swipeControl = new L.Control.Compare(tileLayers[leftKey], tileLayers[rightKey]);
      swipeControl.addTo(map);
      L.DomEvent.disableClickPropagation(swipeControl._container);
      L.DomEvent.on(swipeControl._range, "mousedown", L.DomEvent.stopPropagation);
    }}

    document.getElementById("swipe-left").addEventListener("change", updateSwipe);
    document.getElementById("swipe-right").addEventListener("change", updateSwipe);

    document.getElementById("year-select").addEventListener("change", function() {{
      currentYear = this.value;
      rebuildLayers();
    }});

    ["chk-rgb","chk-geomad","chk-class","chk-classuf","chk-prob"].forEach(function(id) {{
      document.getElementById(id).addEventListener("change", syncLayers);
    }});

    // Tooltip on hover
    var tooltip = document.getElementById("tooltip");
    var tooltipThrottle = null;

    map.on("mousemove", function(e) {{
      tooltip.style.left = (e.originalEvent.pageX + 14) + "px";
      tooltip.style.top = (e.originalEvent.pageY + 14) + "px";

      if (tooltipThrottle) return;
      tooltipThrottle = setTimeout(function() {{ tooltipThrottle = null; }}, 1000);

      var activeKeys = Object.keys(tileLayers);
      if (activeKeys.length === 0) {{ tooltip.style.display = "none"; return; }}

      var promises = activeKeys.map(function(key) {{
        var L_ = LAYERS[key];
        var url = "/mosaic/point/" + e.latlng.lng.toFixed(6) + "," + e.latlng.lat.toFixed(6)
          + "?dataset=" + L_.ds + "&year=" + currentYear + "&" + L_.qs;
        return fetch(url).then(function(r) {{ return r.ok ? r.json() : null; }}).catch(function() {{ return null; }});
      }});

      Promise.all(promises).then(function(results) {{
        var lines = [];
        for (var i = 0; i < activeKeys.length; i++) {{
          var key = activeKeys[i];
          var data = results[i];
          if (!data || !data.assets || !data.assets.length) continue;
          var asset = data.assets[0];
          if (!asset || !asset.values) continue;
          var label = LAYERS[key].label;
          var vals = asset.values;
          // Skip nodata pixels
          if (vals.every(function(v) {{ return v === 0 || v === 255 || v === null; }})) continue;
          if (key === "class" || key === "classuf") {{
            var v = vals[0];
            if (v === 255 || v === 0) continue;
            var cls = CLASS_LABELS[v] || ("Class " + v);
            lines.push(label + ": " + cls + " (" + v + ")");
          }} else if (key === "prob") {{
            lines.push(label + ": " + (vals[0] !== undefined ? vals[0] + "%" : "N/A"));
          }} else {{
            var bandDescs = asset.band_descriptions || asset.band_names || [];
            var parts = [];
            for (var j = 0; j < vals.length; j++) {{
              var bn = bandDescs[j] || ("b" + (j+1));
              // band_descriptions are arrays like ["red_b1"], take first element if array
              if (Array.isArray(bn)) bn = bn[0];
              // Strip trailing _b1 suffix
              bn = bn.replace(/_b\d+$/, "");
              parts.push(bn + "=" + (typeof vals[j] === "number" ? vals[j].toFixed(0) : vals[j]));
            }}
            lines.push(label + ": " + parts.join(", "));
          }}
        }}
        if (lines.length > 0) {{
          tooltip.textContent = lines.join("\\n");
          tooltip.style.display = "block";
        }} else {{
          tooltip.style.display = "none";
        }}
      }});
    }});

    map.on("mouseout", function() {{
      tooltip.style.display = "none";
    }});

    // --- URL hash state ---
    function getStateFromHash() {{
      var hash = window.location.hash.replace(/^#/, "");
      if (!hash) return null;
      var parts = hash.split("/");
      var state = {{}};
      // lat,lng,zoom
      if (parts[0]) {{
        var pos = parts[0].split(",");
        if (pos.length >= 3) {{
          state.lat = parseFloat(pos[0]);
          state.lng = parseFloat(pos[1]);
          state.zoom = parseInt(pos[2], 10);
        }}
      }}
      if (parts[1]) state.year = parts[1];
      if (parts[2]) state.basemap = parts[2];
      if (parts[3]) state.layers = parts[3].split(",");
      if (parts[4]) {{
        var opParts = parts[4].split(",");
        state.opacity = {{}};
        opParts.forEach(function(p) {{
          var kv = p.split(":");
          if (kv.length === 2) state.opacity[kv[0]] = parseFloat(kv[1]);
        }});
      }}
      if (parts[5]) {{
        var sw = parts[5].split(",");
        state.swipeLeft = sw[0] || "";
        state.swipeRight = sw[1] || "";
      }}
      return state;
    }}

    function pushState() {{
      var c = map.getCenter();
      var z = map.getZoom();
      var pos = c.lat.toFixed(4) + "," + c.lng.toFixed(4) + "," + z;
      var year = currentYear;
      var basemap = document.getElementById("basemap-select").value;
      var layers = [];
      for (var key in CHECKBOX_MAP) {{
        if (document.getElementById(CHECKBOX_MAP[key]).checked) layers.push(key);
      }}
      var opParts = [];
      for (var k in tileLayers) {{
        opParts.push(k + ":" + tileLayers[k].options.opacity);
      }}
      var sl = document.getElementById("swipe-left").value || "";
      var sr = document.getElementById("swipe-right").value || "";
      var swipe = (sl || sr) ? sl + "," + sr : "";
      var hash = pos + "/" + year + "/" + basemap + "/" + layers.join(",") + "/" + opParts.join(",") + "/" + swipe;
      history.replaceState(null, "", "#" + hash);
    }}

    // Apply hash state on load
    var initState = getStateFromHash();
    if (initState) {{
      if (initState.lat !== undefined) map.setView([initState.lat, initState.lng], initState.zoom);
      if (initState.year && document.querySelector('#year-select option[value="' + initState.year + '"]')) {{
        currentYear = initState.year;
        document.getElementById("year-select").value = initState.year;
      }}
      if (initState.basemap) {{
        document.getElementById("basemap-select").value = initState.basemap;
        setBasemap(initState.basemap);
      }}
      if (initState.layers) {{
        for (var key in CHECKBOX_MAP) {{
          document.getElementById(CHECKBOX_MAP[key]).checked = initState.layers.indexOf(key) >= 0;
        }}
      }}
    }}

    // Initial build
    rebuildLayers();

    // Apply opacity and swipe from hash after layers are built
    if (initState) {{
      if (initState.opacity) {{
        for (var k in initState.opacity) {{
          if (tileLayers[k]) tileLayers[k].setOpacity(initState.opacity[k]);
        }}
        rebuildOpacitySliders();
      }}
      if (initState.swipeLeft) document.getElementById("swipe-left").value = initState.swipeLeft;
      if (initState.swipeRight) document.getElementById("swipe-right").value = initState.swipeRight;
      if (initState.swipeLeft && initState.swipeRight) updateSwipe();
    }}

    // Update hash on map move and state changes
    map.on("moveend", pushState);
    document.getElementById("year-select").addEventListener("change", pushState);
    document.getElementById("basemap-select").addEventListener("change", pushState);
    document.getElementById("swipe-left").addEventListener("change", pushState);
    document.getElementById("swipe-right").addEventListener("change", pushState);
    ["chk-rgb","chk-geomad","chk-class","chk-classuf","chk-prob"].forEach(function(id) {{
      document.getElementById(id).addEventListener("change", pushState);
    }});
  </script>
</body>
</html>"""

    return HTMLResponse(content=html)


handler = Mangum(app, lifespan="off")
