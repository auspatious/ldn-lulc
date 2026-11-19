import type { DatasetId } from "./datasets.js";
import { DATASETS } from "./datasets.js";
import type { StacItem } from "./stac.js";

export const DATASET_IDS = Object.keys(DATASETS) as DatasetId[];

/** Load state of one dataset's STAC items. */
export type DatasetState =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "loaded"; items: StacItem[] };

/** Load state per dataset version, keyed by `stateKey`, absent until first enabled. */
export type DatasetStates = Partial<Record<string, DatasetState>>;

/** Key of a dataset version in `DatasetStates`. */
export const stateKey = (id: DatasetId, version: string) => `${id}@${version}`;

/** Load state of a dataset at this side's selected version. */
export function datasetState(
  side: SideConfig,
  states: DatasetStates,
  id: DatasetId,
): DatasetState | undefined {
  return states[stateKey(id, side.version[id])];
}

/** Layer settings for one map (or one side of the compare view). */
export type SideConfig = {
  /** Which datasets are shown. */
  enabled: Record<DatasetId, boolean>;
  /** Layer opacity per dataset, 0 to 1. */
  opacity: Record<DatasetId, number>;
  /** Selected version per dataset. */
  version: Record<DatasetId, string>;
  /** Selected year, or null for the latest available. */
  year: number | null;
};

/** A record with one value per dataset. */
const perDataset = <T>(value: (id: DatasetId) => T) =>
  Object.fromEntries(DATASET_IDS.map((id) => [id, value(id)])) as Record<
    DatasetId,
    T
  >;

export const DEFAULT_SIDE: SideConfig = {
  enabled: perDataset(() => true),
  opacity: perDataset(() => 1),
  version: perDataset((id) => DATASETS[id].versions[0]),
  year: null,
};

/** Loaded items for a dataset if it is enabled on this side. */
export function sideItems(
  side: SideConfig,
  states: DatasetStates,
  id: DatasetId,
): StacItem[] {
  const state = datasetState(side, states, id);
  return side.enabled[id] && state?.status === "loaded" ? state.items : [];
}

/**
 * Years available across a side's enabled datasets, and the year to show:
 * the selected year if available, else the latest year every enabled dataset
 * has (e.g. LULC test runs have fewer years than GeoMAD), else the latest.
 */
export function sideYears(side: SideConfig, states: DatasetStates) {
  const yearSets = DATASET_IDS.map(
    (id) => new Set(sideItems(side, states, id).map((i) => i.year)),
  ).filter((s) => s.size > 0);
  const years = [...new Set(yearSets.flatMap((s) => [...s]))].sort(
    (a, b) => a - b,
  );
  const shared = years.filter((y) => yearSets.every((s) => s.has(y)));
  const activeYear =
    side.year !== null && years.includes(side.year)
      ? side.year
      : (shared[shared.length - 1] ?? years[years.length - 1] ?? null);
  return { years, activeYear };
}
