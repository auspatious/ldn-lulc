import { LoadingWidget } from "@deck.gl/widgets";
import type { MultiCOGLayerProps } from "@developmentseed/deck.gl-geotiff";
import { MosaicLayer, MultiCOGLayer } from "@developmentseed/deck.gl-geotiff";
import type { RasterModule } from "@developmentseed/deck.gl-raster";
import type { Device } from "@luma.gl/core";
import type { CSSProperties, Ref } from "react";
import { useMemo, useState } from "react";
import type {
  MapRef,
  ViewState,
  ViewStateChangeEvent,
} from "react-map-gl/maplibre";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import type { DatasetId } from "./datasets.js";
import { createLulcPaletteTexture, DATASETS } from "./datasets.js";
import { epsgResolver } from "./proj.js";
import { DeckGlOverlay, loadingWidgetProps } from "./shared/index.js";
import type { DatasetStates, SideConfig } from "./side.js";
import { DATASET_IDS, sideItems, sideYears } from "./side.js";
import type { StacItem } from "./stac.js";

// ponytail: fixed per-item cache budgets, so worst-case GPU memory is about
// MAX_CACHED_ITEMS * MAX_ITEM_CACHE_BYTES per map. Tune if memory or reloads are a problem.
/** Items whose opened COGs and textures stay alive after leaving the view. */
const MAX_CACHED_ITEMS = 50;
/** Texture budget per item, in bytes. */
const MAX_ITEM_CACHE_BYTES = 16 * 1024 ** 2;

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

export interface MapPaneProps {
  side: SideConfig;
  states: DatasetStates;
  mapRef: Ref<MapRef>;
  /** View used when the map mounts. Later moves are synced via `mapRef`. */
  initialViewState: Partial<ViewState>;
  onMove: (e: ViewStateChangeEvent) => void;
  onMoveEnd?: () => void;
  style?: CSSProperties;
}

/** A MapLibre map with its own deck.gl overlay rendering one side's layers. */
export function MapPane({
  side,
  states,
  mapRef,
  initialViewState,
  onMove,
  onMoveEnd,
  style,
}: MapPaneProps) {
  const [device, setDevice] = useState<Device | null>(null);

  // Each map has its own GPU device, so textures and pipelines are per pane.
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

  // Memoised so map moves don't rebuild layers.
  const layers = useMemo(() => {
    const { activeYear } = sideYears(side, states);
    if (!pipelines) {
      return [];
    }
    return DATASET_IDS.flatMap((id) => {
      const dataset = DATASETS[id];
      const sources = sideItems(side, states, id).filter(
        (i) => i.year === activeYear,
      );
      if (sources.length === 0) {
        return [];
      }
      return new MosaicLayer<StacItem>({
        id: `${id}-${activeYear}`,
        sources,
        maxCacheSize: MAX_CACHED_ITEMS,
        renderSource: (item, { signal }) =>
          new MultiCOGLayer({
            id: `cog-${item.id}`,
            sources: getCogSources(item, dataset.bands),
            composite: dataset.composite,
            renderPipeline: pipelines[id],
            // Show source pixels values.
            resampling: "nearest",
            epsgResolver,
            opacity: side.opacity[id],
            maxCacheByteSize: MAX_ITEM_CACHE_BYTES,
            signal,
          }),
        // @ts-expect-error beforeId is injected by @deck.gl/mapbox; LayerProps
        // doesn't know about it.
        beforeId: "boundary_country_outline",
      });
    });
  }, [side, states, pipelines]);

  return (
    <div style={{ position: "absolute", inset: 0, ...style }}>
      <MaplibreMap
        ref={mapRef}
        initialViewState={initialViewState}
        onMove={onMove}
        onMoveEnd={onMoveEnd}
        mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
      >
        <DeckGlOverlay
          layers={layers}
          // With no layers, deck never redraws, so the widget would keep its
          // initial loading state forever.
          widgets={layers.length > 0 ? [loadingWidget] : []}
          interleaved
          onDeviceInitialized={setDevice}
        />
      </MaplibreMap>
    </div>
  );
}
