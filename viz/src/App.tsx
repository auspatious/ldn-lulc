import { GeoJsonLayer } from "@deck.gl/layers";
import {
  COGLayer,
  MosaicLayer,
  MultiCOGLayer,
} from "@developmentseed/deck.gl-geotiff";
import { LinearRescale } from "@developmentseed/deck.gl-raster/gpu-modules";
import type { Device } from "@luma.gl/core";
import type { ShaderModule } from "@luma.gl/shadertools";
import type { StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useMemo, useState } from "react";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import { DeckGLOverlay } from "./deckgl-overlay.js";
import { fetchGeomadItems, GEOMAD_PARQUET_URL, type GeomadItem } from "./geomad.js";
import { queryDistinctYears } from "./duckdb.js";
import { fetchLulcItems, type LulcItem } from "./lulc.js";
import {
  buildLulcColormapTexture,
  getLulcTileData,
  LULC_CLASSES,
  type LulcTileData,
  makeLulcRenderTile,
} from "./lulc-render.js";
import { fetchTilesGeojson } from "./tiles-layer.js";
import {
  type LayerKey,
  type LayerUiState,
  readUrlState,
  writeUrlState,
} from "./url-state.js";

// Matches the existing titiler viewer's rescale=7200,12000 stretch for
// red/green/blue (visualisation/static/index.html). COG pixel values are
// uint16, sampled into the shader as r16unorm (normalized by 65535), so the
// raw DN range is divided through to match.
const RESCALE_MIN = 7200 / 65535;
const RESCALE_MAX = 12000 / 65535;

// ponytail: the texture sampler is hardcoded to bilinear inside
// @developmentseed/deck.gl-geotiff (not exposed as an overridable prop), so
// any nodata/valid boundary arrives at this shader already smoothed into a
// continuous gradient, not a crisp edge. Measured every cutoff from 0 to
// RESCALE_MIN against real tiles: below the cutoff -> transparent gap,
// between the cutoff and RESCALE_MIN -> still opaque black (LinearRescale's
// floor clamps it), above RESCALE_MIN -> real data. There is no cutoff that
// avoids a visible seam, because the seam IS that gradient — this only
// chooses which artifact (transparent vs black) shows at the boundary.
// Picked RESCALE_MIN: transparent reads as "no data" in a map viewer, black
// reads as a rendering bug. Ceiling: a genuinely very dark real pixel (deep
// clear water, shadow) below RESCALE_MIN on all channels renders
// transparent too, same as it would already render as black.
// Upgrade path: needs library support for nearest-neighbor sampling or a
// real mask/alpha band read — not fixable from this application's code.
const NODATA_BLEND_THRESHOLD = RESCALE_MIN;

const DiscardNearZero = {
  name: "discardNearZero",
  fs: `
    uniform discardNearZeroUniforms {
      float threshold;
    } discardNearZero;
  `,
  inject: {
    "fs:DECKGL_FILTER_COLOR": `
      if (max(color.r, max(color.g, color.b)) <= discardNearZero.threshold) {
        discard;
      }
    `,
  },
  uniformTypes: { threshold: "f32" },
  getUniforms: (props?: { threshold?: number }) => ({
    threshold: props?.threshold ?? 0,
  }),
} as const satisfies ShaderModule<{ threshold: number }>;

// Inline raster style instead of a hosted vector style URL — no external
// style-JSON fetch to go wrong, just tiles.
const BASEMAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

// MultiCOGLayer only re-opens/re-fetches its COGs when the `sources` prop
// object is a *different reference* (checked via `!==`, not deep equality —
// see updateState in the compiled multi-cog-layer.js). buildGeomadLayer runs
// again on every opacity/visibility change, so without this cache each
// toggle built a fresh `{red:{url},...}` object every time and triggered a
// full data reload even though the URLs never changed.
const geomadSourcesCache = new Map<
  string,
  { red: { url: string }; green: { url: string }; blue: { url: string } }
>();

function getGeomadSources(source: GeomadItem) {
  let sources = geomadSourcesCache.get(source.id);
  if (!sources) {
    sources = {
      red: { url: source.assets.red.href },
      green: { url: source.assets.green.href },
      blue: { url: source.assets.blue.href },
    };
    geomadSourcesCache.set(source.id, sources);
  }
  return sources;
}

function buildGeomadLayer(items: GeomadItem[], year: number, ui: LayerUiState) {
  return new MosaicLayer<GeomadItem>({
    // Include the year in the id: MosaicLayer's inner TileLayer keeps a
    // stable id across updates (`mosaic-layer-${id}`), so with a constant id
    // here a year switch just updates props on the same persistent tileset
    // instead of unambiguously signaling "this is a new dataset" — changing
    // the id forces a clean unmount of the old year's tiles and mount of the
    // new year's, rather than relying on the library's partial-update path.
    id: `geomad-mosaic-${year}`,
    sources: items,
    visible: ui.visible,
    opacity: ui.opacity,
    renderSource: (source) =>
      new MultiCOGLayer({
        id: `geomad-${source.id}`,
        opacity: ui.opacity,
        sources: getGeomadSources(source),
        composite: { r: "red", g: "green", b: "blue" },
        renderPipeline: [
          {
            module: DiscardNearZero,
            props: { threshold: NODATA_BLEND_THRESHOLD },
          },
          {
            module: LinearRescale,
            props: { rescaleMin: RESCALE_MIN, rescaleMax: RESCALE_MAX },
          },
        ],
      }),
  });
}

function buildLulcLayer(
  items: LulcItem[],
  year: number,
  colormapTexture: ReturnType<typeof buildLulcColormapTexture>,
  ui: LayerUiState,
) {
  const renderTile = makeLulcRenderTile(colormapTexture);
  return new MosaicLayer<LulcItem>({
    id: `lulc-mosaic-${year}`,
    sources: items,
    visible: ui.visible,
    opacity: ui.opacity,
    renderSource: (source) =>
      new COGLayer<LulcTileData>({
        id: `lulc-${source.id}`,
        opacity: ui.opacity,
        geotiff: source.assets.classification.href,
        getTileData: getLulcTileData,
        renderTile,
        onTileUnload: (tile) => {
          (tile.content as LulcTileData | null)?.texture.destroy();
        },
      }),
  });
}

function Legend({ visible }: { visible: boolean }) {
  if (!visible) {
    return null;
  }
  return (
    <div
      style={{
        position: "absolute",
        bottom: 8,
        left: 8,
        zIndex: 10,
        padding: "8px 10px",
        background: "rgba(0,0,0,0.65)",
        color: "#fff",
        font: "12px sans-serif",
        borderRadius: 4,
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 4 }}>LULC</div>
      {LULC_CLASSES.map(({ value, label, color }) => (
        <div
          key={value}
          style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}
        >
          <span
            style={{
              width: 12,
              height: 12,
              background: `rgb(${color.join(",")})`,
              display: "inline-block",
              borderRadius: 2,
            }}
          />
          {label}
        </div>
      ))}
    </div>
  );
}

const LAYER_LABELS: Record<LayerKey, string> = {
  geomad: "GeoMAD",
  lulc: "LULC",
  tiles: "Tile grid",
};

// Top-to-bottom in the controls list = top-to-bottom in the map's stacking
// order (tiles render on top, geomad on the bottom) — reverse of the
// bottom-up order layers are pushed in for rendering.
const LAYER_LIST_ORDER: LayerKey[] = ["tiles", "lulc", "geomad"];

function Controls({
  year,
  years,
  onYearChange,
  layers,
  onLayerChange,
}: {
  year: number;
  years: number[];
  onYearChange: (year: number) => void;
  layers: Record<LayerKey, LayerUiState>;
  onLayerChange: (key: LayerKey, ui: LayerUiState) => void;
}) {
  return (
    <div
      style={{
        position: "absolute",
        top: 8,
        right: 8,
        zIndex: 10,
        padding: "10px 12px",
        background: "rgba(0,0,0,0.65)",
        color: "#fff",
        font: "12px sans-serif",
        borderRadius: 4,
        width: 200,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        <img src="/favicon.svg" alt="Auspatious" width={20} height={22} />
        <a
          href="https://auspatious.com"
          target="_blank"
          rel="noreferrer"
          style={{ color: "#fff", textDecoration: "none", fontWeight: 600 }}
        >
          Auspatious
        </a>
      </div>

      <label style={{ display: "block", marginBottom: 8 }}>
        Year{" "}
        <select
          value={year}
          onChange={(e) => onYearChange(Number(e.target.value))}
          style={{ width: "100%" }}
        >
          {years.map((y) => (
            <option key={y} value={y}>
              {y}
            </option>
          ))}
        </select>
      </label>

      {LAYER_LIST_ORDER.map((key) => {
        const ui = layers[key];
        return (
          <div key={key} style={{ marginBottom: 8 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <input
                type="checkbox"
                checked={ui.visible}
                onChange={(e) =>
                  onLayerChange(key, { ...ui, visible: e.target.checked })
                }
              />
              {LAYER_LABELS[key]}
            </label>
            <input
              type="range"
              min={0}
              max={100}
              value={Math.round(ui.opacity * 100)}
              onChange={(e) =>
                onLayerChange(key, { ...ui, opacity: Number(e.target.value) / 100 })
              }
              style={{ width: "100%" }}
            />
          </div>
        );
      })}
    </div>
  );
}

export default function App() {
  const initial = useMemo(() => readUrlState(), []);

  const [year, setYear] = useState(initial.year);
  const [years, setYears] = useState<number[]>([initial.year]);
  const [layerUi, setLayerUi] = useState(initial.layers);
  const [view, setView] = useState(initial.view);

  const [geomadItems, setGeomadItems] = useState<GeomadItem[]>([]);
  const [lulcItems, setLulcItems] = useState<LulcItem[]>([]);
  const [tilesGeojson, setTilesGeojson] = useState<GeoJSON.FeatureCollection | null>(
    null,
  );

  const [device, setDevice] = useState<Device | null>(null);
  const [status, setStatus] = useState("loading…");

  // Available years for the selector — geomad has the fullest series.
  useEffect(() => {
    queryDistinctYears(GEOMAD_PARQUET_URL)
      .then(setYears)
      .catch((err: unknown) => console.error("Failed to load years:", err));
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setStatus(`loading ${year}…`);
    Promise.all([
      fetchGeomadItems(year, controller.signal),
      fetchLulcItems(year, controller.signal),
    ])
      .then(([geomad, lulc]) => {
        setGeomadItems(geomad);
        setLulcItems(lulc);
        setStatus(`${geomad.length} geomad / ${lulc.length} lulc items (${year})`);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setStatus(`error: ${err instanceof Error ? err.message : String(err)}`);
      });
    return () => controller.abort();
  }, [year]);

  useEffect(() => {
    const controller = new AbortController();
    fetchTilesGeojson(controller.signal)
      .then(setTilesGeojson)
      .catch((err: unknown) => console.error("Failed to load tiles geojson:", err));
    return () => controller.abort();
  }, []);

  const lulcColormapTexture = useMemo(
    () => (device ? buildLulcColormapTexture(device) : null),
    [device],
  );

  // Persist year/view/layer state into the URL whenever it changes.
  useEffect(() => {
    writeUrlState({ year, view, layers: layerUi });
  }, [year, view, layerUi]);

  // Memoized so panning/zooming (which only changes `view`) doesn't rebuild
  // these — without this, every render (including onMoveEnd) constructed
  // brand-new MosaicLayer/MultiCOGLayer/COGLayer instances with fresh
  // renderSource closures, which deck.gl can't distinguish from a real data
  // change, so it re-initialized every visible tile's mesh/texture on every
  // pan/zoom (the "slow and glitchy" symptom).
  const geomadLayer = useMemo(
    () =>
      geomadItems.length > 0
        ? buildGeomadLayer(geomadItems, year, layerUi.geomad)
        : null,
    [geomadItems, year, layerUi.geomad],
  );
  const lulcLayer = useMemo(
    () =>
      lulcItems.length > 0 && lulcColormapTexture
        ? buildLulcLayer(lulcItems, year, lulcColormapTexture, layerUi.lulc)
        : null,
    [lulcItems, year, lulcColormapTexture, layerUi.lulc],
  );
  const tilesLayer = useMemo(
    () =>
      tilesGeojson
        ? new GeoJsonLayer({
            id: "tiles-geojson",
            data: tilesGeojson,
            visible: layerUi.tiles.visible,
            opacity: layerUi.tiles.opacity,
            stroked: true,
            filled: false,
            getLineColor: [255, 140, 0, 200],
            lineWidthMinPixels: 1,
          })
        : null,
    [tilesGeojson, layerUi.tiles],
  );

  const layers = [geomadLayer, lulcLayer, tilesLayer].filter((l) => l !== null);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MaplibreMap
        initialViewState={view}
        mapStyle={BASEMAP_STYLE}
        onMoveEnd={(e) =>
          setView({
            longitude: e.viewState.longitude,
            latitude: e.viewState.latitude,
            zoom: e.viewState.zoom,
            pitch: e.viewState.pitch,
            bearing: e.viewState.bearing,
          })
        }
      >
        <DeckGLOverlay
          layers={layers}
          deviceProps={{ webgl: { antialias: false } }}
          onDeviceInitialized={setDevice}
        />
      </MaplibreMap>

      <div
        style={{
          position: "absolute",
          top: 8,
          left: 8,
          zIndex: 10,
          padding: "6px 10px",
          background: "rgba(0,0,0,0.6)",
          color: "#fff",
          font: "13px sans-serif",
          borderRadius: 4,
        }}
      >
        {status}
      </div>

      <Controls
        year={year}
        years={years}
        onYearChange={setYear}
        layers={layerUi}
        onLayerChange={(key, ui) => setLayerUi((prev) => ({ ...prev, [key]: ui }))}
      />

      <Legend visible={layerUi.lulc.visible} />
    </div>
  );
}
