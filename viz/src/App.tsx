import "@deck.gl/widgets/stylesheet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import type { RefObject } from "react";
import { useEffect, useRef, useState } from "react";
import type { MapRef, ViewStateChangeEvent } from "react-map-gl/maplibre";
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
  const leftMap = useRef<MapRef>(null);
  const rightMap = useRef<MapRef>(null);
  // Set while mirroring a move, so the mirrored map's move event is ignored.
  const syncing = useRef(false);

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

  /** The left map's current camera, or the initial one before it mounts. */
  const currentView = (): MapView => {
    const map = leftMap.current?.getMap();
    if (!map) {
      return initial.view;
    }
    const { lng, lat } = map.getCenter();
    return {
      longitude: lng,
      latitude: lat,
      zoom: map.getZoom(),
      bearing: map.getBearing(),
      pitch: map.getPitch(),
    };
  };

  /** Save the current view and layer settings to the URL. */
  const saveUrl = () =>
    writeUrlState({ view: currentView(), left, right, compare, split });

  // Rewrite the URL whenever layer or compare settings change.
  useEffect(saveUrl, [left, right, compare, split]);

  /** Mirror one map's camera onto the other without re-rendering React. */
  const syncTo =
    (target: RefObject<MapRef | null>) => (e: ViewStateChangeEvent) => {
      const map = target.current;
      if (syncing.current || !map) {
        return;
      }
      const { longitude, latitude, zoom, bearing, pitch } = e.viewState;
      syncing.current = true;
      map.jumpTo({ center: [longitude, latitude], zoom, bearing, pitch });
      syncing.current = false;
    };

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
        side={left}
        states={states}
        mapRef={leftMap}
        initialViewState={initial.view}
        onMove={syncTo(rightMap)}
        // Right map moves are mirrored here, so this covers both sides.
        onMoveEnd={saveUrl}
      />
      {compare ? (
        <>
          <MapPane
            side={right}
            states={states}
            mapRef={rightMap}
            // Start where the left map currently is.
            initialViewState={currentView()}
            onMove={syncTo(leftMap)}
            // Clipped areas don't receive pointer events, so each side
            // drives its own map.
            style={{ clipPath: `inset(0 0 0 ${split * 100}%)` }}
          />
          <SwipeHandle split={split} onChange={setSplit} />
        </>
      ) : null}

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
