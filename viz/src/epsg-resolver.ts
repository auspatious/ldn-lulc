import type { EpsgResolver, ProjectionDefinition } from "@developmentseed/proj";
import { epsgResolver as defaultEpsgResolver } from "@developmentseed/proj";

/**
 * `wkt-parser` mostly passes a PROJJSON `conversion.method.name` straight
 * through as `projName` — fine for the classic ESRI-style short names proj4
 * registers projections under, but wrong for PROJJSON (what the default,
 * epsg.io-backed resolver returns), which uses full human-readable EPSG
 * method names instead. proj4 has no projection registered under those, so
 * `proj4(def, ...)` fails with "Could not get projection name from:
 * [object Object]".
 *
 * Add an entry here whenever a PROJJSON method name used by our data is
 * confirmed to hit this gap; keyed by proj4's own registered short name
 * (`node_modules/proj4/lib/projections/<name>.js`'s `export var names`).
 */
const PROJJSON_METHOD_NAME_TO_PROJ4_NAME: Record<string, string> = {
  // EPSG:6933 (WGS 84 / NSIDC EASE-Grid 2.0 Global) — used by the
  // non-Pacific LULC/GeoMAD region — and other EASE-Grid 2.0 CRSes use this
  // method; proj4 only registers it as "cea".
  "Lambert Cylindrical Equal Area": "cea",
};

/**
 * Second, deeper bug in the same area: proj4's `cea.js` reads its standard
 * parallel from `this.lat_ts` (`Math.sin(this.lat_ts)` in `init()`), but
 * `wkt-parser` only ever sets `lat1`/`latitude_of_1st_standard_parallel` —
 * the generic name it uses for every method's "1st standard parallel"
 * parameter, which happens to be `lat_ts` specifically for `cea`. Left
 * unset, `Math.sin(undefined)` is `NaN`, which poisons proj4's internal
 * scale factor (`k0`) and silently makes *every* forward/inverse call
 * return `[NaN, NaN]` — including at the projection's own origin `(0, 0)`.
 * Confirmed directly: without this fix, `proj4(def, "EPSG:4326").forward([0,
 * 0])` returns `[NaN, NaN]`; with it, `[0, 0]` as expected, and real tile
 * corners reproject to exactly the STAC item's own bbox.
 */
function fixCeaLatTs(def: ProjectionDefinition): void {
  if (def.projName === "cea" && def.lat_ts === undefined) {
    def.lat_ts = def.lat1;
  }
}

/**
 * Wraps the default `@developmentseed/proj` EPSG resolver, patching known
 * `wkt-parser` gaps for CRSes our data uses. Pass as the `epsgResolver` prop
 * to `COGLayer`/`MultiCOGLayer`.
 */
export const epsgResolverWithFixups: EpsgResolver = async (epsg) => {
  const def = await defaultEpsgResolver(epsg);

  const proj4Name =
    def.projName !== undefined
      ? PROJJSON_METHOD_NAME_TO_PROJ4_NAME[def.projName]
      : undefined;
  if (proj4Name !== undefined) {
    def.projName = proj4Name;
  }

  fixCeaLatTs(def);

  return def;
};
