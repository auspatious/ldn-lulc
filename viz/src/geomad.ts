import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

const GEOMAD_PARQUET_URL =
  "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet";

// Render one recent year rather than every year in the file (~800 items/year
// across ~26 years) — keeps the mosaic to a single, non-overlapping time slice.
const YEAR = 2025;

export type GeomadItem = {
  id: string;
  bbox: [number, number, number, number];
  assets: {
    red: { href: string };
    green: { href: string };
    blue: { href: string };
  };
};

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

async function getDb(): Promise<duckdb.AsyncDuckDB> {
  if (!dbPromise) {
    dbPromise = (async () => {
      const bundle = await duckdb.selectBundle({
        mvp: { mainModule: duckdb_wasm, mainWorker: mvp_worker },
        eh: { mainModule: duckdb_wasm_eh, mainWorker: eh_worker },
      });
      const worker = new Worker(bundle.mainWorker!);
      const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      return db;
    })();
  }
  return dbPromise;
}

/** Query the geomad STAC GeoParquet once and return items for MosaicLayer. */
export async function fetchGeomadItems(
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
      WHERE EXTRACT(year FROM datetime) = ${YEAR}
        -- A handful of antimeridian-crossing tiles have a corrupted bbox of
        -- exactly [-180, ymin, 180, ymax] instead of their true footprint
        -- (upstream data bug, not fixable client-side) — drop them rather
        -- than render them stretched across the whole world.
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
