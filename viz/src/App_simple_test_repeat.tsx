import { COGLayer, MosaicLayer } from "@developmentseed/deck.gl-geotiff";
import type { StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { Map as MaplibreMap } from "react-map-gl/maplibre";
import { DeckGLOverlay } from "./deckgl-overlay.js";

type LulcSource = { id: string; bbox: [number, number, number, number]; href: string };

// One real COG on each side of the antimeridian (dep_ls_lulc STAC collection).
const SOURCES: LulcSource[] = [
  {
    id: "dep_ls_lulc_063_019_2025",
    bbox: [177.380649859963, -19.29852907893266, 178.24303253271776, -18.4776694259545],
    href: "https://s3.us-west-2.amazonaws.com/dep-public-staging/dep_ls_lulc/0-0-9/063/019/2025/dep_ls_lulc_063_019_2025_classification.tif",
  },
  {
    id: "dep_ls_lulc_067_019_2025",
    bbox: [-179.169819449018, -19.29852907893266, -178.30743677626327, -18.4776694259545],
    href: "https://s3.us-west-2.amazonaws.com/dep-public-staging/dep_ls_lulc/0-0-9/067/019/2025/dep_ls_lulc_067_019_2025_classification.tif",
  },
];

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

const layer = new MosaicLayer<LulcSource>({
  id: "lulc-mosaic",
  sources: SOURCES,
  renderSource: (source) => new COGLayer({ id: `lulc-${source.id}`, geotiff: source.href }),
});

export default function App() {
  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      <MaplibreMap
        initialViewState={{ longitude: 180, latitude: -18.9, zoom: 7 }}
        mapStyle={BASEMAP_STYLE}
        renderWorldCopies={true}
      >
        <DeckGLOverlay layers={[layer]} />
      </MaplibreMap>
    </div>
  );
}
