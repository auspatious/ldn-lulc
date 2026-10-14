import { LoadingWidget } from "@deck.gl/widgets";
import type { MultiCOGLayerProps } from "@developmentseed/deck.gl-geotiff";
import { MosaicLayer, MultiCOGLayer } from "@developmentseed/deck.gl-geotiff";
import type { RasterModule } from "@developmentseed/deck.gl-raster";
import type { Device } from "@luma.gl/core";
import { DeckGlOverlay, loadingWidgetProps } from "./shared/index.js";
import "@deck.gl/widgets/stylesheet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useMemo, useRef, useState } from "react";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import type { DatasetState } from "./control-panel.js";
import { ControlPanel } from "./control-panel.js";
import type { DatasetId } from "./datasets.js";
import { createLulcPaletteTexture, DATASETS } from "./datasets.js";
import { epsgResolver } from "./proj.js";
import type { StacItem } from "./stac.js";
import { loadStacItems } from "./stac.js";

// ponytail: fixed per-item cache budgets, so worst-case GPU memory is about
// MAX_CACHED_ITEMS * MAX_ITEM_CACHE_BYTES. Tune if memory or reloads are a problem.
/** Items whose opened COGs and textures stay alive after leaving the view. */
const MAX_CACHED_ITEMS = 50;
/** Texture budget per item, in bytes. */
const MAX_ITEM_CACHE_BYTES = 16 * 1024 ** 2;

const DATASET_IDS = Object.keys(DATASETS) as DatasetId[];

/**
 * Per-item MultiCOGLayer `sources` objects. MultiCOGLayer re-opens every COG
 * when `sources` changes identity, so it must be stable across renders.
 */
const cogSourcesCache = new WeakMap<StacItem, MultiCOGLayerProps["sources"]>();

/** Get the cached MultiCOGLayer sources for an item's bands. */
function getCogSources(item: StacItem, bands: string[]) {
  let sources = cogSourcesCache.get(item);
  if (!sources) {
    sources = Object.fromEntries(
      bands.map((band) => [band, { url: item.assets[band] }]),
    );
    cogSourcesCache.set(item, sources);
  }
  return sources;
}

export default function App() {
  const [enabled, setEnabled] = useState<Record<DatasetId, boolean>>({
    geomad: false,
    lulc: true,
  });
  const [opacity, setOpacity] = useState<Record<DatasetId, number>>({
    geomad: 1,
    lulc: 1,
  });
  const [states, setStates] = useState<
    Partial<Record<DatasetId, DatasetState>>
  >({});
  const [year, setYear] = useState<number | null>(null);
  const [device, setDevice] = useState<Device | null>(null);
  // Datasets whose items have been requested, so each parquet is read once.
  const requested = useRef(new Set<DatasetId>());

  // Built once per device so layer props stay referentially stable.
  const pipelines = useMemo(() => {
    if (!device) {
      return null;
    }
    const palette = createLulcPaletteTexture(device);
    return Object.fromEntries(
      DATASET_IDS.map((id) => [id, DATASETS[id].renderPipeline(palette)]),
    ) as Record<DatasetId, RasterModule[]>;
  }, [device]);
  const loadingWidget = useMemo(
    () => new LoadingWidget(loadingWidgetProps),
    [],
  );

  // Load each dataset's items the first time it is enabled.
  useEffect(() => {
    for (const id of DATASET_IDS) {
      if (!enabled[id] || requested.current.has(id)) {
        continue;
      }
      requested.current.add(id);
      setStates((s) => ({ ...s, [id]: { status: "loading" } }));
      loadStacItems(DATASETS[id].urls, DATASETS[id].bands)
        .then((items) =>
          setStates((s) => ({ ...s, [id]: { status: "loaded", items } })),
        )
        .catch((err) => {
          console.error(`Error loading ${id} STAC items:`, err);
          const error = err instanceof Error ? err.message : String(err);
          setStates((s) => ({ ...s, [id]: { status: "error", error } }));
        });
    }
  }, [enabled]);

  const loadedItems = (id: DatasetId) => {
    const state = states[id];
    return enabled[id] && state?.status === "loaded" ? state.items : [];
  };

  // Union of years across enabled datasets. Default to the latest.
  const years = [
    ...new Set(DATASET_IDS.flatMap((id) => loadedItems(id).map((i) => i.year))),
  ].sort((a, b) => a - b);
  const activeYear =
    year !== null && years.includes(year)
      ? year
      : (years[years.length - 1] ?? null);

  const layers = [];
  for (const id of DATASET_IDS) {
    const dataset = DATASETS[id];
    const sources = loadedItems(id).filter((i) => i.year === activeYear);
    if (sources.length === 0 || !pipelines) {
      continue;
    }
    layers.push(
      new MosaicLayer<StacItem>({
        id: `${id}-${activeYear}`,
        sources,
        maxCacheSize: MAX_CACHED_ITEMS,
        renderSource: (item, { signal }) =>
          new MultiCOGLayer({
            id: `cog-${item.id}`,
            sources: getCogSources(item, dataset.bands),
            composite: dataset.composite,
            renderPipeline: pipelines[id],
            epsgResolver,
            opacity: opacity[id],
            maxCacheByteSize: MAX_ITEM_CACHE_BYTES,
            signal,
          }),
        // @ts-expect-error beforeId is injected by @deck.gl/mapbox; LayerProps
        // doesn't know about it.
        beforeId: "boundary_country_outline",
      }),
    );
  }

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MaplibreMap
        initialViewState={{ longitude: 0, latitude: 0, zoom: 3 }}
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
      >
        <DeckGlOverlay
          layers={layers}
          // With no layers, deck never redraws, so the widget would keep its
          // initial loading state forever.
          widgets={
layers.length > 0 ? [loadingWidget] : []
          }
          interleaved
          onDeviceInitialized={setDevice}
        />
      </MaplibreMap>

      <ControlPanel
        enabled={enabled}
        onEnabledChange={(id, on) => setEnabled((e) => ({ ...e, [id]: on }))}
        opacity={opacity}
        onOpacityChange={(id, value) =>
          setOpacity((o) => ({ ...o, [id]: value }))
        }
        states={states}
        years={years}
        year={activeYear}
        onYearChange={setYear}
      />
    </div>
  );
}
