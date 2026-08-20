import {
  COGLayer,
  MosaicLayer,
  MultiCOGLayer,
} from "@developmentseed/deck.gl-geotiff";
import { LinearRescale } from "@developmentseed/deck.gl-raster/gpu-modules";
import type { Device } from "@luma.gl/core";
import type { ShaderModule } from "@luma.gl/shadertools";
import Compare from "@maplibre/maplibre-gl-compare";
import "@maplibre/maplibre-gl-compare/dist/maplibre-gl-compare.css";
import type { StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useMemo, useRef, useState } from "react";
import { Map as MaplibreMap, type MapRef } from "react-map-gl/maplibre";
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
import {
  type CompareContent,
  type CompareDataset,
  type CompareState,
  type LayerKey,
  type LayerUiState,
  type MapViewState,
  readUrlState,
  writeUrlState,
} from "./url-state.js";

// LULC predictions only exist for these two years (unlike geomad, which has
// a full annual series) — see visualisation/app.py's LULC_VERSION and the
// years actually present in the STAC GeoParquets (queried directly).
const LULC_YEARS = [2000, 2025];

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

/** One side of the swipe compare — its own map, own device/layer, fetched independently of the main view. */
function CompareSide({
  content,
  layerUi,
  view,
  mapRef,
  wrapperRef,
  onLoad,
}: {
  content: CompareContent;
  layerUi: Record<LayerKey, LayerUiState>;
  view: MapViewState;
  mapRef: React.RefObject<MapRef | null>;
  wrapperRef: React.RefObject<HTMLDivElement | null>;
  onLoad: () => void;
}) {
  const [geomadItems, setGeomadItems] = useState<GeomadItem[]>([]);
  const [lulcItems, setLulcItems] = useState<LulcItem[]>([]);
  const [device, setDevice] = useState<Device | null>(null);
  // Tracks which (dataset, year) the items currently in state actually came
  // from. Without this, changing `content.year` recomputes `layer` on the
  // SAME render using the still-stale items from the previous year (the
  // fetch hasn't resolved yet) — producing a layer id that matches the NEW
  // year but sources from the OLD one, a real mismatch that only
  // self-corrects once the fetch finishes. That transient wrong-data flash
  // is what looked like "doesn't change / desyncs from the select".
  const [loadedContent, setLoadedContent] = useState<CompareContent | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    if (content.dataset === "geomad") {
      fetchGeomadItems(content.year, controller.signal)
        .then((items) => {
          setGeomadItems(items);
          setLoadedContent(content);
        })
        .catch((err: unknown) => {
          if (err instanceof Error && err.name === "AbortError") return;
          console.error("Failed to load compare geomad items:", err);
        });
    } else {
      fetchLulcItems(content.year, controller.signal)
        .then((items) => {
          setLulcItems(items);
          setLoadedContent(content);
        })
        .catch((err: unknown) => {
          if (err instanceof Error && err.name === "AbortError") return;
          console.error("Failed to load compare lulc items:", err);
        });
    }
    return () => controller.abort();
  }, [content.dataset, content.year]);

  const lulcColormapTexture = useMemo(
    () => (device ? buildLulcColormapTexture(device) : null),
    [device],
  );

  const layer = useMemo(() => {
    if (
      !loadedContent ||
      loadedContent.dataset !== content.dataset ||
      loadedContent.year !== content.year
    ) {
      return null;
    }
    if (content.dataset === "geomad") {
      return geomadItems.length > 0
        ? buildGeomadLayer(geomadItems, content.year, layerUi.geomad)
        : null;
    }
    return lulcItems.length > 0 && lulcColormapTexture
      ? buildLulcLayer(lulcItems, content.year, lulcColormapTexture, layerUi.lulc)
      : null;
  }, [
    content.dataset,
    content.year,
    loadedContent,
    geomadItems,
    lulcItems,
    lulcColormapTexture,
    layerUi.geomad,
    layerUi.lulc,
  ]);

  return (
    <div ref={wrapperRef} style={{ position: "absolute", inset: 0 }}>
      <MaplibreMap
        ref={mapRef}
        initialViewState={view}
        mapStyle={BASEMAP_STYLE}
        onLoad={onLoad}
        // maplibre-gl-compare clips each map via the legacy `clip: rect()`
        // CSS property (not `clip-path`), which is a documented no-op on
        // anything that isn't position:absolute/fixed. react-map-gl's
        // container defaults to position:relative, so without this override
        // the clip was silently doing nothing — the divider dragged fine,
        // but nothing was ever actually masked.
        style={{ position: "absolute", inset: 0 }}
      >
        <DeckGLOverlay
          layers={layer ? [layer] : []}
          deviceProps={{ webgl: { antialias: false } }}
          onDeviceInitialized={setDevice}
        />
      </MaplibreMap>
    </div>
  );
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
  const [compare, setCompare] = useState<CompareState>(initial.compare);

  const leftMapRef = useRef<MapRef>(null);
  const rightMapRef = useRef<MapRef>(null);
  const leftWrapperRef = useRef<HTMLDivElement>(null);
  const rightWrapperRef = useRef<HTMLDivElement>(null);
  const compareContainerRef = useRef<HTMLDivElement>(null);
  const [leftLoaded, setLeftLoaded] = useState(false);
  const [rightLoaded, setRightLoaded] = useState(false);

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

  // Reset the "map loaded" flags whenever compare mode is toggled off/on, so
  // a re-enable doesn't try to reuse stale refs from the previous mount.
  useEffect(() => {
    setLeftLoaded(false);
    setRightLoaded(false);
  }, [compare.enabled]);

  // Instantiate maplibre-gl-compare once both compare maps have loaded.
  useEffect(() => {
    if (!compare.enabled || !leftLoaded || !rightLoaded) {
      return;
    }
    const leftMap = leftMapRef.current?.getMap();
    const rightMap = rightMapRef.current?.getMap();
    const container = compareContainerRef.current;
    if (!leftMap || !rightMap || !container) {
      return;
    }
    const instance = new Compare(leftMap, rightMap, container);

    // maplibre-gl-compare only clips with the legacy `clip: rect()` CSS
    // property, which is paint-only — it does NOT restrict pointer events
    // (unlike `clip-path`, which does). That alone wouldn't even be enough
    // here: `clip`/`clip-path` on map.getContainer() only affects that
    // element's own subtree, but each side is ALSO wrapped in a plain,
    // fully-overlapping `<div>` of our own (CompareSide's root) — an
    // unclipped sibling still claims every pointer event across its whole
    // box regardless of what's clipped inside it. The right wrapper, being
    // later in the DOM, was capturing all events everywhere, including the
    // region where only the left map is visible. Mirror the library's
    // position as `clip-path` on our OWN wrapper divs (not the inner map
    // containers) so each side's actual hit area matches what's drawn.
    const leftEl = leftMap.getContainer();
    const rightEl = rightMap.getContainer();
    const leftWrapper = leftWrapperRef.current;
    const rightWrapper = rightWrapperRef.current;
    const syncClipPath = () => {
      if (!leftWrapper || !rightWrapper) {
        return;
      }
      // biome-ignore lint: instance.currentPosition/_bounds aren't in the .d.ts shim, but are plain public-in-practice fields set by the library itself.
      const inst = instance as unknown as {
        currentPosition?: number;
        _bounds?: { width: number };
      };
      const x = inst.currentPosition;
      const width = inst._bounds?.width;
      if (x === undefined || width === undefined) {
        return;
      }
      leftWrapper.style.clipPath = `inset(0 ${Math.max(0, width - x)}px 0 0)`;
      rightWrapper.style.clipPath = `inset(0 0 0 ${x}px)`;
    };
    syncClipPath();
    const observer = new MutationObserver(syncClipPath);
    observer.observe(leftEl, { attributes: true, attributeFilter: ["style"] });
    observer.observe(rightEl, { attributes: true, attributeFilter: ["style"] });

    return () => {
      observer.disconnect();
      if (leftWrapper) leftWrapper.style.clipPath = "";
      if (rightWrapper) rightWrapper.style.clipPath = "";
      instance.remove();
    };
  }, [compare.enabled, leftLoaded, rightLoaded]);

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
  const layers = [geomadLayer, lulcLayer].filter((l) => l !== null);

  return (
    <div style={{ position: "relative", width: "100%", height: "100%", overflow: "hidden" }}>
      {compare.enabled ? (
        <div ref={compareContainerRef} style={{ position: "absolute", inset: 0 }}>
          <CompareSide
            content={compare.left}
            layerUi={layerUi}
            view={view}
            mapRef={leftMapRef}
            wrapperRef={leftWrapperRef}
            onLoad={() => setLeftLoaded(true)}
          />
          <CompareSide
            content={compare.right}
            layerUi={layerUi}
            view={view}
            mapRef={rightMapRef}
            wrapperRef={rightWrapperRef}
            onLoad={() => setRightLoaded(true)}
          />
        </div>
      ) : (
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
