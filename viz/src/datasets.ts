import type { RasterModule } from "@developmentseed/deck.gl-raster";
import {
  FilterNoDataVal,
  LinearRescale,
} from "@developmentseed/deck.gl-raster/gpu-modules";
import type { Device, Texture } from "@luma.gl/core";
import type { ShaderModule } from "@luma.gl/shadertools";

/** Level 1 LULC classes. Mirrors ldn/typology_mapping.yaml. */
export const LULC_CLASSES = [
  { value: 1, label: "Tree cover", color: [0, 100, 0] },
  { value: 2, label: "Grassland", color: [255, 255, 76] },
  { value: 3, label: "Cropland", color: [240, 150, 255] },
  { value: 4, label: "Wetland", color: [0, 150, 160] },
  { value: 5, label: "Built-up", color: [250, 0, 0] },
  { value: 6, label: "Water", color: [0, 100, 200] },
  { value: 7, label: "Other", color: [180, 180, 180] },
] as const;

/** Build a 256x1 RGBA lookup texture indexed by class value. Unlisted values are transparent. */
export function createLulcPaletteTexture(device: Device): Texture {
  const data = new Uint8Array(256 * 4);
  for (const { value, color } of LULC_CLASSES) {
    data.set([...color, 255], value * 4);
  }
  return device.createTexture({
    data,
    format: "rgba8unorm",
    width: 256,
    height: 1,
    sampler: { minFilter: "nearest", magFilter: "nearest" },
  });
}

/**
 * Map an r8unorm class value in `color.r` to its palette colour, discarding
 * transparent entries (nodata and unknown classes).
 */
const LulcPalette = {
  name: "lulcPalette",
  inject: {
    "fs:#decl": "uniform sampler2D paletteTexture;",
    "fs:DECKGL_FILTER_COLOR": /* glsl */ `
      color = texelFetch(paletteTexture, ivec2(int(round(color.r * 255.0)), 0), 0);
      if (color.a == 0.0) {
        discard;
      }
    `,
  },
  getUniforms: (props: Partial<{ paletteTexture: Texture }>) => ({
    paletteTexture: props.paletteTexture,
  }),
} as const satisfies ShaderModule<{ paletteTexture: Texture }>;

/** Landsat C2 surface reflectance DN for reflectance r, as r16unorm. */
const reflectance = (r: number) => (r / 0.0000275 + 7273) / 65535;

export type DatasetId = "lulc" | "geomad";

type Dataset = {
  label: string;
  /** stac-geoparquet files to combine into one mosaic. */
  urls: string[];
  /** Asset keys loaded per item, passed to MultiCOGLayer as source names. */
  bands: string[];
  composite: { r: string; g?: string; b?: string };
  renderPipeline: (palette: Texture) => RasterModule[];
};

/** Render order: later entries draw on top. */
export const DATASETS: Record<DatasetId, Dataset> = {
  geomad: {
    label: "GeoMAD true colour",
    urls: [
      "https://data.source.coop/auspatious/geomad-sids/ls_geomad/0-2-1/ls_geomad.parquet",
    ],
    bands: ["red", "green", "blue"],
    composite: { r: "red", g: "green", b: "blue" },
    renderPipeline: () => [
      { module: FilterNoDataVal, props: { value: 0 } },
      {
        module: LinearRescale,
        props: { rescaleMin: reflectance(0), rescaleMax: reflectance(0.2) },
      },
    ],
  },
  lulc: {
    label: "Land use / land cover",
    urls: [
      "https://dep-public-staging.s3.us-west-2.amazonaws.com/dep_ls_lulc/0-0-9/dep_ls_lulc.parquet",
      "https://dep-public-staging.s3.us-west-2.amazonaws.com/ci_ls_lulc/0-0-9/ci_ls_lulc.parquet",
    ],
    bands: ["classification"],
    composite: { r: "classification" },
    renderPipeline: (palette) => [
      { module: LulcPalette, props: { paletteTexture: palette } },
    ],
  },
};
