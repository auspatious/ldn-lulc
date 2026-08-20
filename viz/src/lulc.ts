import { getDb } from "./duckdb.js";

// LULC is published as two regional STAC GeoParquets (pacific + non-pacific)
// under ldn/utils.py's get_analysis_epsg regions — same split as app.py's
// LULC_REGIONAL_PREFIXES. Query both and union; item ids are already
// prefixed per region (dep_ls_lulc_*/ci_ls_lulc_*) so there's no collision.
export const LULC_PARQUET_URLS = [
  "https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_lulc/0-0-9/dep_ls_lulc.parquet",
  "https://dep-public-staging.s3.us-west-2.amazonaws.com/ci_ls_lulc/0-0-9/ci_ls_lulc.parquet",
];

export type LulcItem = {
  id: string;
  bbox: [number, number, number, number];
  assets: {
    classification: { href: string };
  };
};

async function queryOne(
  url: string,
  year: number,
  signal?: AbortSignal,
): Promise<LulcItem[]> {
  const db = await getDb();
  const conn = await db.connect();
  try {
    const result = await conn.query(`
      SELECT
        id,
        bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax,
        assets.classification.href AS href
      FROM read_parquet('${url}')
      WHERE EXTRACT(year FROM datetime) = ${year}
        -- Same antimeridian-crossing bbox bug as geomad — drop rows whose
        -- bbox is corrupted to the full [-180, 180] range.
        AND (bbox.xmax - bbox.xmin) < 10
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
      assets: { classification: { href: row.href as string } },
    }));
  } finally {
    await conn.close();
  }
}

/** Query both LULC regional STAC GeoParquets for one year and union the items. */
export async function fetchLulcItems(
  year: number,
  signal?: AbortSignal,
): Promise<LulcItem[]> {
  const regions = await Promise.all(
    LULC_PARQUET_URLS.map((url) => queryOne(url, year, signal)),
  );
  return regions.flat();
}
