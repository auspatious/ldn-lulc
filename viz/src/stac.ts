import type { FileMetaData, SchemaElement } from "hyparquet";
import {
  asyncBufferFromUrl,
  parquetMetadataAsync,
  parquetReadObjects,
} from "hyparquet";
import { compressors } from "hyparquet-compressors";

/** The subset of a STAC item the app needs. */
export type StacItem = {
  id: string;
  /** WGS84 bounds as [minX, minY, maxX, maxY]. */
  bbox: [number, number, number, number];
  year: number;
  /** Asset key to COG href. */
  assets: Record<string, string>;
};

/** A stac-geoparquet row, limited to the columns read here. */
type StacRow = {
  id: string;
  datetime: Date;
  bbox: { xmin: number; ymin: number; xmax: number; ymax: number };
  assets: Record<string, { href: string }>;
};

// ponytail: hyparquet only selects top-level columns, which would download the
// large per-asset statistics in `assets`. Delete selectLeafColumns once
// hyparquet accepts nested paths like "assets.red.href".
/**
 * Keep only the given leaf column paths (e.g. "assets.red.href") in the file
 * metadata, so hyparquet fetches and decodes nothing else.
 */
function selectLeafColumns(
  metadata: FileMetaData,
  leaves: Set<string>,
): FileMetaData {
  const [root, ...elements] = metadata.schema;
  let i = 0;
  /** Walk one schema subtree depth-first, returning its kept elements. */
  const prune = (parent: string): SchemaElement[] => {
    const element = elements[i++];
    const path = parent ? `${parent}.${element.name}` : element.name;
    if (!element.num_children) {
      return leaves.has(path) ? [element] : [];
    }
    const children: SchemaElement[][] = [];
    for (let c = 0; c < element.num_children; c++) {
      const kept = prune(path);
      if (kept.length > 0) {
        children.push(kept);
      }
    }
    return children.length > 0
      ? [{ ...element, num_children: children.length }, ...children.flat()]
      : [];
  };
  const fields: SchemaElement[][] = [];
  while (i < elements.length) {
    const kept = prune("");
    if (kept.length > 0) {
      fields.push(kept);
    }
  }
  return {
    ...metadata,
    schema: [{ ...root, num_children: fields.length }, ...fields.flat()],
    row_groups: metadata.row_groups.map((group) => ({
      ...group,
      columns: group.columns.filter((chunk) =>
        leaves.has(chunk.meta_data?.path_in_schema.join(".") ?? ""),
      ),
    })),
  };
}

/** Read the needed columns of one stac-geoparquet file over HTTP range requests. */
async function readStacRows(url: string, leaves: Set<string>) {
  const file = await asyncBufferFromUrl({ url });
  const metadata = selectLeafColumns(await parquetMetadataAsync(file), leaves);
  return (await parquetReadObjects({
    file,
    metadata,
    compressors,
  })) as StacRow[];
}

/** Read STAC items from one or more stac-geoparquet files. */
export async function loadStacItems(
  urls: string[],
  bands: string[],
): Promise<StacItem[]> {
  const leaves = new Set([
    "id",
    "datetime",
    "bbox.xmin",
    "bbox.ymin",
    "bbox.xmax",
    "bbox.ymax",
    ...bands.map((b) => `assets.${b}.href`),
  ]);
  const files = await Promise.all(urls.map((u) => readStacRows(u, leaves)));
  return files.flat().map((row) => ({
    id: row.id,
    bbox: [row.bbox.xmin, row.bbox.ymin, row.bbox.xmax, row.bbox.ymax],
    year: row.datetime.getUTCFullYear(),
    assets: Object.fromEntries(bands.map((b) => [b, row.assets[b].href])),
  }));
}
