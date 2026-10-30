import "@deck.gl/widgets/stylesheet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useRef, useState } from "react";
import type { MapRef } from "react-map-gl/maplibre";
import { ControlPanel } from "./control-panel.js";
import type { DatasetId } from "./datasets.js";
import { DATASETS } from "./datasets.js";
import { MapPane } from "./map-pane.js";
import type { DatasetStates, SideConfig } from "./side.js";
import { DATASET_IDS } from "./side.js";
import { loadStacItems } from "./stac.js";
import { SwipeHandle } from "./swipe-handle.js";
import type { MapView } from "./url-state.js";
import { readUrlState, writeUrlState } from "./url-state.js";

/** A map's current camera, or `fallback` before the map mounts. */
function mapView(ref: MapRef | null, fallback: MapView): MapView {
  const map = ref?.getMap();
  if (!map) {
    return fallback;
  }
  const { lng, lat } = map.getCenter();
  return {
    longitude: lng,
    latitude: lat,
    zoom: map.getZoom(),
    bearing: map.getBearing(),
    pitch: map.getPitch(),
  };
}

export default function App() {
  // Restore a shared map from the URL on first load.
  const [initial] = useState(readUrlState);
  const [left, setLeft] = useState<SideConfig>(initial.left);
  const [right, setRight] = useState<SideConfig>(initial.right);
  const [compare, setCompare] = useState(initial.compare);
  const [split, setSplit] = useState(initial.split);
  const [states, setStates] = useState<DatasetStates>({});
  // Datasets whose items have been requested, so each parquet is read once.
  const requested = useRef(new Set<DatasetId>());
  const mapRef = useRef<MapRef>(null);

  // Load each dataset's items the first time either side enables it.
  useEffect(() => {
    for (const id of DATASET_IDS) {
      const wanted = left.enabled[id] || (compare && right.enabled[id]);
      if (!wanted || requested.current.has(id)) {
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
  }, [left, right, compare]);

  /** Save the current view and layer settings to the URL. */
  const saveUrl = useCallback(
    () =>
      writeUrlState({
        view: mapView(mapRef.current, initial.view),
        left,
        right,
        compare,
        split,
      }),
    [initial.view, left, right, compare, split],
  );

  // Rewrite the URL whenever layer or compare settings change.
  useEffect(saveUrl, [saveUrl]);

  /** Start the right side from the left side's settings. */
  const handleCompareChange = (on: boolean) => {
    if (on) {
      setRight(left);
    }
    setCompare(on);
  };

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MapPane
        left={left}
        right={compare ? right : null}
        split={split}
        states={states}
        mapRef={mapRef}
        initialViewState={initial.view}
        onMoveEnd={saveUrl}
      />
      {compare ? <SwipeHandle split={split} onChange={setSplit} /> : null}

      <ControlPanel
        left={left}
        onLeftChange={setLeft}
        right={right}
        onRightChange={setRight}
        compare={compare}
        onCompareChange={handleCompareChange}
        states={states}
      />
    </div>
  );
}
