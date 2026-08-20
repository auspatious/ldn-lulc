import * as duckdb from "@duckdb/duckdb-wasm";
import duckdb_wasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvp_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdb_wasm_eh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import eh_worker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;

/** Shared duckdb-wasm instance — one WASM worker for every geoparquet query. */
export async function getDb(): Promise<duckdb.AsyncDuckDB> {
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

/** Distinct years present in a STAC GeoParquet's `datetime` column. */
export async function queryDistinctYears(url: string): Promise<number[]> {
  const db = await getDb();
  const conn = await db.connect();
  try {
    const result = await conn.query(`
      SELECT DISTINCT EXTRACT(year FROM datetime) AS yr
      FROM read_parquet('${url}')
      ORDER BY yr
    `);
    return result.toArray().map((row) => Number(row.yr));
  } finally {
    await conn.close();
  }
}
