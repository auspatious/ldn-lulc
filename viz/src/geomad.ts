import { getDb } from "./duckdb.js";

export const GEOMAD_PARQUET_URL =
  "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet";

export type GeomadItem = {
  id: string;
  bbox: [number, number, number, number];
  assets: {
    red: { href: string };
    green: { href: string };
    blue: { href: string };
  };
};

/** Query the geomad STAC GeoParquet for one year and return items for MosaicLayer. */
export async function fetchGeomadItems(
  year: number,
  signal?: AbortSignal,
): Promise<GeomadItem[]> {
  const db = await getDb();
  const conn = await db.connect();
  try {
    const result = await conn.query(`
      SELECT
        id,
        bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax,
        assets.red.href AS red_href,
        assets.green.href AS green_href,
        assets.blue.href AS blue_href
      FROM read_parquet('${GEOMAD_PARQUET_URL}')
      WHERE EXTRACT(year FROM datetime) = ${year}
        -- Antimeridian-crossing heuristic - simply ignore for now.
        AND NOT (bbox.xmax = 180 AND bbox.xmin = -180)
    `);
    signal?.throwIfAborted();
    return result.toArray().map((row) => ({
      id: row.id as string,
      bbox: [row.xmin, row.ymin, row.xmax, row.ymax] as [
        number,
        number,
        number,
        number,
      ],
      assets: {
        red: { href: row.red_href as string },
        green: { href: row.green_href as string },
        blue: { href: row.blue_href as string },
      },
    }));
  } finally {
    await conn.close();
  }
}
