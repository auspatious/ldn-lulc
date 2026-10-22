import { LoadingWidget } from "@deck.gl/widgets";
import type { MultiCOGLayerProps } from "@developmentseed/deck.gl-geotiff";
import { MosaicLayer, MultiCOGLayer } from "@developmentseed/deck.gl-geotiff";
import type { RasterModule } from "@developmentseed/deck.gl-raster";
import type { Device } from "@luma.gl/core";
import type { Ref } from "react";
import { useMemo, useState } from "react";
import type { MapRef, ViewState } from "react-map-gl/maplibre";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import type { DatasetId } from "./datasets.js";
import { createLulcPaletteTexture, DATASETS } from "./datasets.js";
import { epsgResolver } from "./proj.js";
import { DeckGlOverlay, loadingWidgetProps } from "./shared/index.js";
import type { DatasetStates, SideConfig } from "./side.js";
import { DATASET_IDS, sideItems, sideYears } from "./side.js";
import { SplitClipExtension } from "./split-clip.js";
import type { StacItem } from "./stac.js";

// ponytail: fixed per-item cache budgets, so worst-case GPU memory is about
// MAX_CACHED_ITEMS * MAX_ITEM_CACHE_BYTES per dataset per side. Tune if memory or reloads are a problem.
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

const CLIP = {
  left: [new SplitClipExtension({ side: "left" })],
  right: [new SplitClipExtension({ side: "right" })],
};

/**
 * One mosaic layer per enabled dataset on a side, at the side's active year.
 * With a split, layers are clipped to the side's half of the divider.
 */
function sideLayers(
  name: "left" | "right",
  side: SideConfig | null,
  states: DatasetStates,
  pipelines: Record<DatasetId, RasterModule[]> | null,
  split: number | null,
) {
  if (!side || !pipelines) {
    return [];
  }
  const { activeYear } = sideYears(side, states);
  const clip = split === null ? {} : { extensions: CLIP[name], splitPosition: split };
  return DATASET_IDS.flatMap((id) => {
    const dataset = DATASETS[id];
    const sources = sideItems(side, states, id).filter(
      (i) => i.year === activeYear,
    );
    if (sources.length === 0) {
      return [];
    }
    return new MosaicLayer<StacItem>({
      id: `${name}-${id}-${activeYear}`,
      sources,
      maxCacheSize: MAX_CACHED_ITEMS,
      renderSource: (item, { signal }) =>
        new MultiCOGLayer({
          id: `${name}-cog-${item.id}`,
          sources: getCogSources(item, dataset.bands),
          composite: dataset.composite,
          renderPipeline: pipelines[id],
          // Show source pixels values.
          resampling: "nearest",
          epsgResolver,
          opacity: side.opacity[id],
          maxCacheByteSize: MAX_ITEM_CACHE_BYTES,
          signal,
          ...clip,
        }),
      // @ts-expect-error beforeId is injected by @deck.gl/mapbox; LayerProps
      // doesn't know about it.
      beforeId: "boundary_country_outline",
    });
  });
}

export interface MapPaneProps {
  left: SideConfig;
  /** Right side of the swipe compare, or null when not comparing. */
  right: SideConfig | null;
  /** Swipe divider position, 0 to 1. */
  split: number;
  states: DatasetStates;
  mapRef: Ref<MapRef>;
  /** View used when the map mounts. */
  initialViewState: Partial<ViewState>;
  onMoveEnd: () => void;
}

/**
 * A MapLibre map with a deck.gl overlay. When comparing, both sides render on
 * this one map, each clipped to its side of the divider.
 */
export function MapPane({
  left,
  right,
  split,
  states,
  mapRef,
  initialViewState,
  onMoveEnd,
}: MapPaneProps) {
  const [device, setDevice] = useState<Device | null>(null);

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

  // Memoised per side so map moves, and changes to the other side, don't
  // rebuild a side's layers.
  const compareSplit = right ? split : null;
  const leftLayers = useMemo(
    () => sideLayers("left", left, states, pipelines, compareSplit),
    [left, states, pipelines, compareSplit],
  );
  const rightLayers = useMemo(
    () => sideLayers("right", right, states, pipelines, compareSplit),
    [right, states, pipelines, compareSplit],
  );
  const layers = useMemo(
    () => [...leftLayers, ...rightLayers],
    [leftLayers, rightLayers],
  );

  return (
    <div style={{ position: "absolute", inset: 0 }}>
      <MaplibreMap
        ref={mapRef}
        initialViewState={initialViewState}
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
