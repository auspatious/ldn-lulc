import proj4 from "proj4";

// Hard-code the CRSs used by the DEP and CI grids so we don't depend on epsg.io at runtime.
proj4.defs(
  "EPSG:3832",
  "+proj=merc +lon_0=150 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs",
);
proj4.defs(
  "EPSG:6933",
  "+proj=cea +lat_ts=30 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs",
);

/** Resolve an EPSG code to its proj4 definition. */
export async function epsgResolver(epsg: number) {
  const code = `EPSG:${epsg}`;
  const proj = proj4.defs(code);
  if (!proj) {
    throw new Error(`EPSG code ${code} not found in proj4 definitions`);
  }
  return proj;
}
