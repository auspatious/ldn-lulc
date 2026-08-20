export const TILES_GEOJSON_URL =
  "https://raw.githubusercontent.com/auspatious/ldn-lulc/refs/heads/main/ldn/sids_all_tiles.geojson";

export async function fetchTilesGeojson(
  signal?: AbortSignal,
): Promise<GeoJSON.FeatureCollection> {
  const resp = await fetch(TILES_GEOJSON_URL, { signal });
  if (!resp.ok) {
    throw new Error(`Failed to fetch tiles geojson: ${resp.status}`);
  }
  return resp.json();
}
