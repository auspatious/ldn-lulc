import type { DatasetId } from "./datasets.js";
import { DATASETS } from "./datasets.js";
import type { StacItem } from "./stac.js";

export const DATASET_IDS = Object.keys(DATASETS) as DatasetId[];

/** Load state of one dataset's STAC items. */
export type DatasetState =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "loaded"; items: StacItem[] };

/** Load state per dataset, absent until first enabled. */
export type DatasetStates = Partial<Record<DatasetId, DatasetState>>;

/** Layer settings for one map (or one side of the compare view). */
export type SideConfig = {
  /** Which datasets are shown. */
  enabled: Record<DatasetId, boolean>;
  /** Layer opacity per dataset, 0 to 1. */
  opacity: Record<DatasetId, number>;
  /** Selected year, or null for the latest available. */
  year: number | null;
};

export const DEFAULT_SIDE: SideConfig = {
  enabled: { geomad: true, lulc: true },
  opacity: { geomad: 1, lulc: 1 },
  year: null,
};

/** Loaded items for a dataset if it is enabled on this side. */
export function sideItems(
  side: SideConfig,
  states: DatasetStates,
  id: DatasetId,
): StacItem[] {
  const state = states[id];
  return side.enabled[id] && state?.status === "loaded" ? state.items : [];
}

/**
 * Years available across a side's enabled datasets, and the year to show:
 * the selected year if available, else the latest.
 */
export function sideYears(side: SideConfig, states: DatasetStates) {
  const years = [
    ...new Set(
      DATASET_IDS.flatMap((id) =>
        sideItems(side, states, id).map((i) => i.year),
      ),
    ),
  ].sort((a, b) => a - b);
  const activeYear =
    side.year !== null && years.includes(side.year)
      ? side.year
      : (years[years.length - 1] ?? null);
  return { years, activeYear };
}
