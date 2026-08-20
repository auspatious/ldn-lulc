export type LayerKey = "geomad" | "lulc" | "tiles";

export type LayerUiState = { visible: boolean; opacity: number };

export type MapViewState = {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch: number;
  bearing: number;
};

export type UrlState = {
  year: number;
  view: MapViewState;
  layers: Record<LayerKey, LayerUiState>;
};

export const DEFAULT_VIEW: MapViewState = {
  longitude: 178,
  latitude: -17,
  zoom: 4,
  pitch: 0,
  bearing: 0,
};

export const DEFAULT_YEAR = 2025;

const LAYER_KEYS: LayerKey[] = ["geomad", "lulc", "tiles"];

function parseNum(raw: string | null, fallback: number): number {
  if (raw === null) {
    return fallback;
  }
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

function parseLayerParam(params: URLSearchParams, key: LayerKey): LayerUiState {
  const raw = params.get(key);
  if (!raw) {
    return { visible: true, opacity: 1 };
  }
  const [visRaw, opRaw] = raw.split(",");
  const visible = visRaw !== "0";
  const opacityPercent = Number(opRaw);
  const opacity = Number.isFinite(opacityPercent)
    ? Math.min(1, Math.max(0, opacityPercent / 100))
    : 1;
  return { visible, opacity };
}

/** Read year/view/layer state from the current URL, falling back to defaults. */
export function readUrlState(): UrlState {
  const params = new URLSearchParams(window.location.search);
  const layers = Object.fromEntries(
    LAYER_KEYS.map((key) => [key, parseLayerParam(params, key)]),
  ) as Record<LayerKey, LayerUiState>;
  return {
    year: parseNum(params.get("year"), DEFAULT_YEAR),
    view: {
      longitude: parseNum(params.get("lng"), DEFAULT_VIEW.longitude),
      latitude: parseNum(params.get("lat"), DEFAULT_VIEW.latitude),
      zoom: parseNum(params.get("zoom"), DEFAULT_VIEW.zoom),
      pitch: parseNum(params.get("pitch"), DEFAULT_VIEW.pitch),
      bearing: parseNum(params.get("bearing"), DEFAULT_VIEW.bearing),
    },
    layers,
  };
}

/** Replace the current URL's query string with the given state (no history entry). */
export function writeUrlState(state: UrlState): void {
  const params = new URLSearchParams();
  params.set("year", String(state.year));
  params.set("lng", state.view.longitude.toFixed(5));
  params.set("lat", state.view.latitude.toFixed(5));
  params.set("zoom", state.view.zoom.toFixed(2));
  params.set("pitch", state.view.pitch.toFixed(2));
  params.set("bearing", state.view.bearing.toFixed(2));
  for (const key of LAYER_KEYS) {
    const { visible, opacity } = state.layers[key];
    params.set(key, `${visible ? 1 : 0},${Math.round(opacity * 100)}`);
  }
  const url = `${window.location.pathname}?${params.toString()}`;
  window.history.replaceState(null, "", url);
}
