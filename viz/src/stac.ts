import * as duckdb from "@duckdb/duckdb-wasm";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import ehWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import mvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";

/** The subset of a STAC item the app needs. */
export type StacItem = {
  id: string;
  /** WGS84 bounds as [minX, minY, maxX, maxY]. */
  bbox: [number, number, number, number];
  year: number;
  /** Asset key to COG href. */
  assets: Record<string, string>;
};

let connection: Promise<duckdb.AsyncDuckDBConnection> | null = null;

/** Start DuckDB once and share the connection. */
function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  connection ??= (async () => {
    const bundle = await duckdb.selectBundle({
      mvp: { mainModule: mvpWasm, mainWorker: mvpWorker },
      eh: { mainModule: ehWasm, mainWorker: ehWorker },
    });
    if (!bundle.mainWorker) {
      throw new Error("No DuckDB worker for this browser");
    }
    const db = new duckdb.AsyncDuckDB(
      new duckdb.VoidLogger(),
      new Worker(bundle.mainWorker),
    );
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return db.connect();
  })();
  return connection;
}

/**
 * Read STAC items from one or more stac-geoparquet files.
 *
 * DuckDB reads only the nested columns selected here over HTTP range requests,
 * skipping the large per-asset statistics in `assets`.
 */
export async function loadStacItems(
  urls: string[],
  bands: string[],
): Promise<StacItem[]> {
  const conn = await getConnection();
  const files = urls.map((u) => `'${u}'`).join(", ");
  const hrefs = bands.map((b) => `assets."${b}".href AS "${b}"`).join(", ");
  const table = await conn.query(`
    SELECT id, bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax,
      year(datetime)::INTEGER AS year, ${hrefs}
    FROM read_parquet([${files}])
  `);
  return table.toArray().map((row) => ({
    id: row.id,
    bbox: [row.xmin, row.ymin, row.xmax, row.ymax],
    year: row.year,
    assets: Object.fromEntries(bands.map((b) => [b, row[b]])),
  }));
}
