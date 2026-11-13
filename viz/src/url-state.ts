import type { DatasetId } from "./datasets.js";
import type { SideConfig } from "./side.js";
import { DATASET_IDS, DEFAULT_SIDE } from "./side.js";

/** Camera position, as stored in the URL. */
export type MapView = {
  longitude: number;
  latitude: number;
  zoom: number;
  bearing: number;
  pitch: number;
};

/** Everything needed to restore a shared map. */
export type UrlState = {
  view: MapView;
  left: SideConfig;
  right: SideConfig;
  compare: boolean;
  /** Swipe divider position, 0 to 1. */
  split: number;
};

const DEFAULT_VIEW: MapView = {
  longitude: 0,
  latitude: 0,
  zoom: 3,
  bearing: 0,
  pitch: 0,
};

/** Clamp a number to a range. */
const clamp = (n: number, min: number, max: number) =>
  Math.min(Math.max(n, min), max);

/** Parse `zoom/lat/lng[/bearing/pitch]`, falling back to the default view. */
function parseView(value: string | null): MapView {
  const [zoom, latitude, longitude, bearing = 0, pitch = 0] = (value ?? "")
    .split("/")
    .map(Number);
  if (
    ![zoom, latitude, longitude, bearing, pitch].every(Number.isFinite) ||
    zoom === undefined ||
    latitude === undefined ||
    longitude === undefined
  ) {
    return DEFAULT_VIEW;
  }
  return {
    zoom: clamp(zoom, 0, 24),
    latitude: clamp(latitude, -90, 90),
    longitude,
    bearing,
    pitch: clamp(pitch, 0, 85),
  };
}

/**
 * Parse a side from `id:opacityPercent,...` (enabled datasets only) and an
 * optional year param. Returns null if the side is absent from the URL.
 */
function parseSide(params: URLSearchParams, key: string): SideConfig | null {
  const value = params.get(key);
  if (value === null) {
    return null;
  }
  const enabled = { geomad: false, lulc: false };
  const opacity = { geomad: 1, lulc: 1 };
  for (const part of value.split(",")) {
    const [id, percent] = part.split(":");
    if (!DATASET_IDS.includes(id as DatasetId)) {
      continue;
    }
    enabled[id as DatasetId] = true;
    const n = Number(percent);
    if (percent && Number.isFinite(n)) {
      opacity[id as DatasetId] = clamp(n / 100, 0, 1);
    }
  }
  const year = Number(params.get(`${key}y`));
  return {
    enabled,
    opacity,
    year: params.has(`${key}y`) && Number.isInteger(year) ? year : null,
  };
}

/** Read the shared map state from the current URL. */
export function readUrlState(): UrlState {
  const params = new URLSearchParams(window.location.search);
  const left = parseSide(params, "l") ?? DEFAULT_SIDE;
  const split = Number(params.get("s"));
  return {
    view: parseView(params.get("v")),
    left,
    right: parseSide(params, "r") ?? left,
    compare: params.get("c") === "1",
    split: params.has("s") && Number.isFinite(split) ? clamp(split, 0, 1) : 0.5,
  };
}

/** Encode a side as `[key, value]` URL params. */
function sideParams(key: string, side: SideConfig): [string, string][] {
  const datasets = DATASET_IDS.filter((id) => side.enabled[id])
    .map((id) => `${id}:${Math.round(side.opacity[id] * 100)}`)
    .join(",");
  const params: [string, string][] = [[key, datasets]];
  if (side.year !== null) {
    params.push([`${key}y`, String(side.year)]);
  }
  return params;
}

let writeTimer: ReturnType<typeof setTimeout> | undefined;

/**
 * Replace the URL with the given state, debounced. Safari throws if
 * `history.replaceState` is called too often, e.g. on every drag frame.
 */
export function writeUrlState(state: UrlState) {
  clearTimeout(writeTimer);
  writeTimer = setTimeout(() => {
    const { view, left, right, compare, split } = state;
    const v = [
      view.zoom.toFixed(2),
      view.latitude.toFixed(4),
      view.longitude.toFixed(4),
    ];
    if (view.bearing !== 0 || view.pitch !== 0) {
      v.push(view.bearing.toFixed(1), view.pitch.toFixed(1));
    }
    const params: [string, string][] = [
      ["v", v.join("/")],
      ...sideParams("l", left),
    ];
    if (compare) {
      params.push(["c", "1"], ...sideParams("r", right), [
        "s",
        split.toFixed(3),
      ]);
    }
    // Built by hand so `,:/` stay readable instead of being percent-encoded.
    const query = params.map(([k, v]) => `${k}=${v}`).join("&");
    window.history.replaceState(null, "", `?${query}`);
  }, 250);
}
