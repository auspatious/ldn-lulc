import { MosaicLayer, MultiCOGLayer } from "@developmentseed/deck.gl-geotiff";
import {
  FilterNoDataVal,
  LinearRescale,
} from "@developmentseed/deck.gl-raster/gpu-modules";
import type { StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useState } from "react";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import { DeckGLOverlay } from "./deckgl-overlay.js";
import { fetchGeomadItems, type GeomadItem } from "./geomad.js";

// Matches the existing titiler viewer's rescale=7200,12000 stretch for
// red/green/blue (visualisation/static/index.html). COG pixel values are
// uint16, sampled into the shader as r16unorm (normalized by 65535), so the
// raw DN range is divided through to match.
const RESCALE_MIN = 7200 / 65535;
const RESCALE_MAX = 12000 / 65535;

// Inline raster style instead of a hosted vector style URL — no external
// style-JSON fetch to go wrong, just tiles.
const BASEMAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};

function buildMosaicLayer(items: GeomadItem[]) {
  return new MosaicLayer<GeomadItem>({
    id: "geomad-mosaic",
    sources: items,
    renderSource: (source) =>
      new MultiCOGLayer({
        id: `geomad-${source.id}`,
        sources: {
          red: { url: source.assets.red.href },
          green: { url: source.assets.green.href },
          blue: { url: source.assets.blue.href },
        },
        composite: { r: "red", g: "green", b: "blue" },
        renderPipeline: [
          { module: FilterNoDataVal, props: { value: 0 } },
          {
            module: LinearRescale,
            props: { rescaleMin: RESCALE_MIN, rescaleMax: RESCALE_MAX },
          },
        ],
      }),
  });
}

export default function App() {
  const [items, setItems] = useState<GeomadItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    fetchGeomadItems(controller.signal)
      .then(setItems)
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, []);

  const layers = items.length > 0 ? [buildMosaicLayer(items)] : [];

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MaplibreMap
        initialViewState={{ longitude: 178, latitude: -17, zoom: 4 }}
        mapStyle={BASEMAP_STYLE}
      >
        <DeckGLOverlay layers={layers} />
      </MaplibreMap>

      <div
        style={{
          position: "absolute",
          top: 8,
          left: 8,
          padding: "6px 10px",
          background: "rgba(0,0,0,0.6)",
          color: "#fff",
          font: "13px sans-serif",
          borderRadius: 4,
        }}
      >
        {loading && "loading geomad STAC items…"}
        {error && `error: ${error}`}
        {!loading && !error && `${items.length} geomad items (2025)`}
      </div>
    </div>
  );
}
