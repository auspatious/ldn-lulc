import "@deck.gl/widgets/stylesheet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useRef, useState } from "react";
import type { MapRef } from "react-map-gl/maplibre";
import { ControlPanel } from "./components/controls/control-panel.js";
import { SwipeHandle } from "./components/controls/swipe-handle.js";
import { MapPane } from "./components/map/map-pane.js";
import { DATASETS } from "./data/datasets.js";
import type { DatasetStates, SideConfig } from "./data/side.js";
import { DATASET_IDS, stateKey } from "./data/side.js";
import { loadStacItems } from "./data/stac.js";
import type { MapView } from "./data/url-state.js";
import { readUrlState, writeUrlState } from "./data/url-state.js";

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
  const [grid, setGrid] = useState(initial.grid);
  const [states, setStates] = useState<DatasetStates>({});
  // Dataset versions whose items have been requested, so each parquet is read once.
  const requested = useRef(new Set<string>());
  const mapRef = useRef<MapRef>(null);

  // Load each dataset version's items the first time either side enables it.
  useEffect(() => {
    const sides = compare ? [left, right] : [left];
    for (const side of sides) {
      for (const id of DATASET_IDS) {
        const version = side.version[id];
        const key = stateKey(id, version);
        if (!side.enabled[id] || requested.current.has(key)) {
          continue;
        }
        requested.current.add(key);
        setStates((s) => ({ ...s, [key]: { status: "loading" } }));
        loadStacItems(DATASETS[id].urls(version), DATASETS[id].bands)
          .then((items) =>
            setStates((s) => ({ ...s, [key]: { status: "loaded", items } })),
          )
          .catch((err) => {
            console.error(`Error loading ${key} STAC items:`, err);
            const error = err instanceof Error ? err.message : String(err);
            setStates((s) => ({ ...s, [key]: { status: "error", error } }));
          });
      }
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
        grid,
      }),
    [initial.view, left, right, compare, split, grid],
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
        grid={grid}
        states={states}
        mapRef={mapRef}
        initialViewState={initial.view}
        onMoveEnd={saveUrl}
      />
      {compare && <SwipeHandle split={split} onChange={setSplit} />}

      <ControlPanel
        left={left}
        onLeftChange={setLeft}
        right={right}
        onRightChange={setRight}
        compare={compare}
        onCompareChange={handleCompareChange}
        grid={grid}
        onGridChange={setGrid}
        states={states}
      />
    </div>
  );
}
