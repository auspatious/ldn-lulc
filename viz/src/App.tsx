import { WebMercatorViewport } from "@deck.gl/core";
import { ClipExtension } from "@deck.gl/extensions";
import {
  COGLayer,
  MosaicLayer,
  MultiCOGLayer,
} from "@developmentseed/deck.gl-geotiff";
import { LinearRescale } from "@developmentseed/deck.gl-raster/gpu-modules";
import type { Device, Texture } from "@luma.gl/core";
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
import { SwipeHandle } from "./swipe-handle.js";
import {
  type CompareContent,
  type CompareDataset,
  type CompareState,
  type LayerKey,
  type LayerUiState,
  readUrlState,
  writeUrlState,
} from "./url-state.js";

// Standard spherical Web Mercator (EPSG:3857) constants — same values
// @developmentseed/deck.gl-raster's RasterTileLayer uses internally to
// position its mesh (raster-tile-layer/constants.js: TILE_SIZE=512,
// WEB_MERCATOR_METER_CIRCUMFERENCE=40075016.686). Not re-exported by the
// package, so duplicated here rather than importing an internal path.
const WEB_MERCATOR_METER_CIRCUMFERENCE = 40075016.686;
const WEB_MERCATOR_RADIUS = WEB_MERCATOR_METER_CIRCUMFERENCE / (2 * Math.PI);
// Half the projected world's vertical extent, in meters — large enough to
// always cover the full globe, used as an unclipped Y range (we only ever
// clip on X).
const WEB_MERCATOR_HALF_EXTENT = WEB_MERCATOR_METER_CIRCUMFERENCE / 2;

/**
 * ClipExtension's `clipBounds` get re-projected through the layer's own
 * `projectPosition` before comparison (see ClipExtension.draw() in
 * @deck.gl/extensions). RasterTileLayer positions its mesh with
 * `coordinateSystem: "cartesian"` + a modelMatrix that scales Web Mercator
 * *meters* into deck.gl's [0, 512] common space — so clipBounds must be
 * given in those same meters, not already-common-space numbers (passing
 * common-space numbers here get scaled down to a sliver near the center,
 * which is why nothing rendered at first: everything got clipped away).
 */
function splitWebMercatorMetersX(
  viewport: WebMercatorViewport,
  splitFraction: number,
): number {
  const splitPx = viewport.width * splitFraction;
  const [lng] = viewport.unproject([splitPx, 0]);
  return lng * (Math.PI / 180) * WEB_MERCATOR_RADIUS;
}

// LULC predictions only exist for these two years (unlike geomad)
const LULC_YEARS = [2000, 2025]; // TODO: remove this once all years have been created.

// Rescale=7200,12000 stretch for red/green/blue.
// COG pixel values are
// uint16, sampled into the shader as r16unorm (normalized by 65535), so the
// raw DN range is divided through to match.
const RESCALE_MIN = 7200 / 65535;
const RESCALE_MAX = 12000 / 65535;

const NODATA_BLEND_THRESHOLD = RESCALE_MIN;

// Bilinear sampling blends real data texels with the zero-filled nodata
// padding at the edge of every source (dataset edge, ragged partial tile, or
// the seam between two adjacent COG scenes in the mosaic). A hard discard at
// NODATA_BLEND_THRESHOLD turns that blend into a one-pixel cliff: values just
// below get dropped (see-through "gap"), values just above survive but
// rescale to near-black ("border"). Fading alpha across a small band instead
// of discarding removes the cliff, at the cost of a few edge pixels being
// partially transparent rather than fully opaque or fully see-through.
// Tune to taste — wider hides more artifacting but eats into genuinely dark
// valid pixels near the low end of the rescale window. Only fades *below*
// threshold (never above it) so real in-range data keeps full opacity —
// fading above threshold too washed out the whole image last attempt.
const NODATA_FEATHER = NODATA_BLEND_THRESHOLD * 0.5;

// Flip to false to silence geomad debug output. When on: draws MultiCOGLayer's
// tile-boundary overlay (primary + secondary tile outlines, stitch/UV info)
// and logs per-source GeoTIFF metadata (nodata, tile size, bbox, stored band
// stats) to the console — useful for tracking down the nodata-edge/COG-seam
// artifacts.
const GEOMAD_DEBUG = false;

const FadeNearZero = {
  name: "fadeNearZero",
  fs: `
    uniform fadeNearZeroUniforms {
      float threshold;
      float feather;
    } fadeNearZero;
  `,
  inject: {
    "fs:DECKGL_FILTER_COLOR": `
      float nearZeroValue = max(color.r, max(color.g, color.b));
      color.a *= smoothstep(
        fadeNearZero.threshold - fadeNearZero.feather,
        fadeNearZero.threshold,
        nearZeroValue
      );
      if (color.a <= 0.0) {
        discard;
      }
    `,
  },
  uniformTypes: { threshold: "f32", feather: "f32" },
  getUniforms: (props?: { threshold?: number; feather?: number }) => ({
    threshold: props?.threshold ?? 0,
    feather: props?.feather ?? 0,
  }),
} as const satisfies ShaderModule<{ threshold: number; feather: number }>;

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

// ClipExtension injects `clipBounds`/`clipByInstance` at runtime; neither
// MultiCOGLayerProps nor COGLayerProps know about extension-injected props.
type ClipProps = {
  extensions: ClipExtension[];
  clipBounds: [number, number, number, number];
  clipByInstance: false;
};

function clipProps(clipBounds?: [number, number, number, number]): ClipProps | object {
  return clipBounds
    ? { extensions: [new ClipExtension()], clipBounds, clipByInstance: false }
    : {};
}

function buildGeomadLayer(
  items: GeomadItem[],
  keyPrefix: string,
  ui: LayerUiState,
  clipBounds?: [number, number, number, number],
) {
  return new MosaicLayer<GeomadItem>({
    // Include keyPrefix (year, or side+year in compare mode) in the id:
    // MosaicLayer's inner TileLayer keeps a stable id across updates
    // (`mosaic-layer-${id}`), so with a constant id here a change just
    // updates props on the same persistent tileset instead of unambiguously
    // signaling "this is a new dataset" — changing the id forces a clean
    // unmount of the old tiles and mount of the new ones, rather than
    // relying on the library's partial-update path.
    id: `geomad-mosaic-${keyPrefix}`,
    sources: items,
    visible: ui.visible,
    opacity: ui.opacity,
    renderSource: (source) =>
      new MultiCOGLayer({
        id: `geomad-${keyPrefix}-${source.id}`,
        opacity: ui.opacity,
        sources: getGeomadSources(source),
        composite: { r: "red", g: "green", b: "blue" },
        renderPipeline: [
          {
            module: FadeNearZero,
            props: { threshold: NODATA_BLEND_THRESHOLD, feather: NODATA_FEATHER },
          },
          {
            module: LinearRescale,
            props: { rescaleMin: RESCALE_MIN, rescaleMax: RESCALE_MAX },
          },
        ],
        debug: GEOMAD_DEBUG,
        debugLevel: 3,
        onGeoTIFFLoad: GEOMAD_DEBUG
          ? (sources, { primaryKey, geographicBounds }) => {
              console.groupCollapsed(
                `[geomad debug] ${source.id} opened (primary=${primaryKey})`,
              );
              console.log("geographicBounds", geographicBounds);
              console.log("composite", { r: "red", g: "green", b: "blue" });
              console.log("rescale", {
                rescaleMin: RESCALE_MIN,
                rescaleMax: RESCALE_MAX,
                nodataDiscardThreshold: NODATA_BLEND_THRESHOLD,
              });
              for (const [name, geotiff] of sources) {
                console.log(name, {
                  nodata: geotiff.nodata,
                  bands: geotiff.count,
                  size: `${geotiff.width}x${geotiff.height}`,
                  tileSize: `${geotiff.tileWidth}x${geotiff.tileHeight}`,
                  isTiled: geotiff.isTiled,
                  bbox: geotiff.bbox,
                  offsets: geotiff.offsets,
                  scales: geotiff.scales,
                  storedStats: geotiff.storedStats,
                });
              }
              console.groupEnd();
            }
          : undefined,
        ...clipProps(clipBounds),
      }),
  });
}

function buildLulcLayer(
  items: LulcItem[],
  keyPrefix: string,
  colormapTexture: Texture,
  ui: LayerUiState,
  clipBounds?: [number, number, number, number],
) {
  const renderTile = makeLulcRenderTile(colormapTexture);
  return new MosaicLayer<LulcItem>({
    id: `lulc-mosaic-${keyPrefix}`,
    sources: items,
    visible: ui.visible,
    opacity: ui.opacity,
    renderSource: (source) =>
      new COGLayer<LulcTileData>({
        id: `lulc-${keyPrefix}-${source.id}`,
        opacity: ui.opacity,
        geotiff: source.assets.classification.href,
        getTileData: getLulcTileData,
        renderTile,
        onTileUnload: (tile) => {
          (tile.content as LulcTileData | null)?.texture.destroy();
        },
        ...clipProps(clipBounds),
      }),
  });
}

/** Dispatches to whichever dataset a compare side has selected. */
function buildSideLayer(
  content: CompareContent,
  geomadItems: GeomadItem[],
  lulcItems: LulcItem[],
  keyPrefix: string,
  layerUi: Record<LayerKey, LayerUiState>,
  lulcColormapTexture: Texture | null,
  clipBounds?: [number, number, number, number],
) {
  if (content.dataset === "geomad") {
    return geomadItems.length > 0
      ? buildGeomadLayer(geomadItems, `${keyPrefix}-${content.year}`, layerUi.geomad, clipBounds)
      : null;
  }
  return lulcItems.length > 0 && lulcColormapTexture
    ? buildLulcLayer(
        lulcItems,
        `${keyPrefix}-${content.year}`,
        lulcColormapTexture,
        layerUi.lulc,
        clipBounds,
      )
    : null;
}

/**
 * Fetches items for one compare side. Clears state at the start of every
 * fetch (rather than tracking "which content the current items came from")
 * so a mid-flight year/dataset switch can't render the previous selection's
 * items under the new selection's layer id — the existing `items.length > 0`
 * checks in `buildSideLayer` already gate rendering on real data being ready.
 */
function useCompareItems(content: CompareContent, enabled: boolean) {
  const [geomadItems, setGeomadItems] = useState<GeomadItem[]>([]);
  const [lulcItems, setLulcItems] = useState<LulcItem[]>([]);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    const controller = new AbortController();
    setGeomadItems([]);
    setLulcItems([]);
    if (content.dataset === "geomad") {
      fetchGeomadItems(content.year, controller.signal)
        .then(setGeomadItems)
        .catch((err: unknown) => {
          if (err instanceof Error && err.name === "AbortError") return;
          console.error("Failed to load compare geomad items:", err);
        });
    } else {
      fetchLulcItems(content.year, controller.signal)
        .then(setLulcItems)
        .catch((err: unknown) => {
          if (err instanceof Error && err.name === "AbortError") return;
          console.error("Failed to load compare lulc items:", err);
        });
    }
    return () => controller.abort();
  }, [enabled, content.dataset, content.year]);

  return { geomadItems, lulcItems };
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
};

// Top-to-bottom in the controls list = top-to-bottom in the map's stacking
// order (lulc renders on top, geomad on the bottom) — reverse of the
// bottom-up order layers are pushed in for rendering.
const LAYER_LIST_ORDER: LayerKey[] = ["lulc", "geomad"];

function yearsForDataset(dataset: CompareDataset, geomadYears: number[]): number[] {
  return dataset === "lulc" ? LULC_YEARS : geomadYears;
}

function CompareControls({
  compare,
  geomadYears,
  onChange,
}: {
  compare: CompareState;
  geomadYears: number[];
  onChange: (compare: CompareState) => void;
}) {
  return (
    <div
      style={{
        marginTop: 8,
        paddingTop: 8,
        borderTop: "1px solid rgba(255,255,255,0.25)",
      }}
    >
      <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
        <input
          type="checkbox"
          checked={compare.enabled}
          onChange={(e) => onChange({ ...compare, enabled: e.target.checked })}
        />
        Swipe compare
      </label>
      {compare.enabled &&
        (["left", "right"] as const).map((side) => {
          const content = compare[side];
          const availableYears = yearsForDataset(content.dataset, geomadYears);
          return (
            <div key={side} style={{ marginBottom: 6 }}>
              <div style={{ fontSize: 11, opacity: 0.8, marginBottom: 2 }}>
                {side === "left" ? "Left" : "Right"}
              </div>
              <select
                value={content.dataset}
                onChange={(e) => {
                  const dataset = e.target.value as CompareDataset;
                  const options = yearsForDataset(dataset, geomadYears);
                  const year = options.includes(content.year)
                    ? content.year
                    : options[options.length - 1];
                  onChange({ ...compare, [side]: { dataset, year } });
                }}
                style={{ width: "100%", marginBottom: 2 }}
              >
                <option value="geomad">GeoMAD</option>
                <option value="lulc">LULC</option>
              </select>
              <select
                value={content.year}
                onChange={(e) =>
                  onChange({
                    ...compare,
                    [side]: { ...content, year: Number(e.target.value) },
                  })
                }
                style={{ width: "100%" }}
              >
                {availableYears.map((y) => (
                  <option key={y} value={y}>
                    {y}
                  </option>
                ))}
              </select>
            </div>
          );
        })}
    </div>
  );
}

function Controls({
  year,
  years,
  onYearChange,
  layers,
  onLayerChange,
  compare,
  onCompareChange,
}: {
  year: number;
  years: number[];
  onYearChange: (year: number) => void;
  layers: Record<LayerKey, LayerUiState>;
  onLayerChange: (key: LayerKey, ui: LayerUiState) => void;
  compare: CompareState;
  onCompareChange: (compare: CompareState) => void;
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
          disabled={compare.enabled}
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

      <CompareControls compare={compare} geomadYears={years} onChange={onCompareChange} />
    </div>
  );
}

export default function App() {
  const initial = useMemo(() => readUrlState(), []);

  const [year, setYear] = useState(initial.year);
  const [years, setYears] = useState<number[]>([initial.year]);
  const [layerUi, setLayerUi] = useState(initial.layers);
  const [view, setView] = useState(initial.view);
  const [liveView, setLiveView] = useState(initial.view);
  const [compare, setCompare] = useState<CompareState>(initial.compare);

  const [geomadItems, setGeomadItems] = useState<GeomadItem[]>([]);
  const [lulcItems, setLulcItems] = useState<LulcItem[]>([]);

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

  const lulcColormapTexture = useMemo(
    () => (device ? buildLulcColormapTexture(device) : null),
    [device],
  );

  // Persist year/view/layer/compare state into the URL whenever it changes.
  useEffect(() => {
    writeUrlState({ year, view, layers: layerUi, compare });
  }, [year, view, layerUi, compare]);

  const { geomadItems: leftGeomadItems, lulcItems: leftLulcItems } = useCompareItems(
    compare.left,
    compare.enabled,
  );
  const { geomadItems: rightGeomadItems, lulcItems: rightLulcItems } = useCompareItems(
    compare.right,
    compare.enabled,
  );

  // Clip rectangles for each side, in Web Mercator meters (see
  // splitWebMercatorMetersX), recomputed from the live (continuously-updated)
  // viewport so the split line stays screen-locked while panning/zooming.
  // Cheap: ClipExtension only needs a prop update on the already-open
  // layers, not a data reload (see buildGeomadLayer/buildLulcLayer —
  // sources/geotiff references stay stable across this).
  const clipBounds = useMemo(() => {
    if (!compare.enabled) {
      return null;
    }
    const viewport = new WebMercatorViewport({
      ...liveView,
      width: window.innerWidth,
      height: window.innerHeight,
    });
    const splitX = splitWebMercatorMetersX(viewport, compare.split);
    return {
      left: [
        -WEB_MERCATOR_HALF_EXTENT,
        -WEB_MERCATOR_HALF_EXTENT,
        splitX,
        WEB_MERCATOR_HALF_EXTENT,
      ] as [number, number, number, number],
      right: [
        splitX,
        -WEB_MERCATOR_HALF_EXTENT,
        WEB_MERCATOR_HALF_EXTENT,
        WEB_MERCATOR_HALF_EXTENT,
      ] as [number, number, number, number],
    };
  }, [compare.enabled, compare.split, liveView]);

  // Memoized so panning/zooming (which only changes `view`) doesn't rebuild
  // these — without this, every render (including onMoveEnd) constructed
  // brand-new MosaicLayer/MultiCOGLayer/COGLayer instances with fresh
  // renderSource closures, which deck.gl can't distinguish from a real data
  // change, so it re-initialized every visible tile's mesh/texture on every
  // pan/zoom (the "slow and glitchy" symptom).
  const geomadLayer = useMemo(
    () =>
      geomadItems.length > 0
        ? buildGeomadLayer(geomadItems, String(year), layerUi.geomad)
        : null,
    [geomadItems, year, layerUi.geomad],
  );
  const lulcLayer = useMemo(
    () =>
      lulcItems.length > 0 && lulcColormapTexture
        ? buildLulcLayer(lulcItems, String(year), lulcColormapTexture, layerUi.lulc)
        : null,
    [lulcItems, year, lulcColormapTexture, layerUi.lulc],
  );

  const leftLayer = useMemo(
    () =>
      clipBounds
        ? buildSideLayer(
            compare.left,
            leftGeomadItems,
            leftLulcItems,
            "left",
            layerUi,
            lulcColormapTexture,
            clipBounds.left,
          )
        : null,
    [compare.left, leftGeomadItems, leftLulcItems, layerUi, lulcColormapTexture, clipBounds],
  );
  const rightLayer = useMemo(
    () =>
      clipBounds
        ? buildSideLayer(
            compare.right,
            rightGeomadItems,
            rightLulcItems,
            "right",
            layerUi,
            lulcColormapTexture,
            clipBounds.right,
          )
        : null,
    [compare.right, rightGeomadItems, rightLulcItems, layerUi, lulcColormapTexture, clipBounds],
  );

  const layers = compare.enabled
    ? [leftLayer, rightLayer].filter((l) => l !== null)
    : [geomadLayer, lulcLayer].filter((l) => l !== null);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MaplibreMap
        initialViewState={view}
        mapStyle={BASEMAP_STYLE}
        onMove={(e) => setLiveView(e.viewState)}
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

      {compare.enabled && (
        <SwipeHandle
          fraction={compare.split}
          onChange={(split) => setCompare((prev) => ({ ...prev, split }))}
        />
      )}

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
        compare={compare}
        onCompareChange={setCompare}
      />

      <Legend visible={layerUi.lulc.visible} />
    </div>
  );
}
