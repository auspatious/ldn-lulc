import type { GetTileDataOptions } from "@developmentseed/deck.gl-geotiff";
import type { RenderTileResult } from "@developmentseed/deck.gl-raster";
import type { GeoTIFF, Overview } from "@developmentseed/geotiff";
import type { Device, Texture } from "@luma.gl/core";
import type { ShaderModule } from "@luma.gl/shadertools";

// Same classes/colors as visualisation/app.py's `cmap` dict (server-side
// titiler colormap). 255 = nodata; anything not listed here also renders
// transparent (the palette texture below is zero-initialized).
export const LULC_CLASSES: Array<{
  value: number;
  label: string;
  color: [number, number, number];
}> = [
  { value: 1, label: "Tree cover", color: [0, 100, 0] },
  { value: 2, label: "Grassland", color: [255, 255, 76] },
  { value: 3, label: "Cropland", color: [240, 150, 255] },
  { value: 4, label: "Wetland", color: [0, 150, 160] },
  { value: 5, label: "Built-up", color: [250, 0, 0] },
  { value: 6, label: "Water", color: [0, 100, 200] },
  { value: 7, label: "Other", color: [180, 180, 180] },
];

/** 256x1 rgba8unorm palette texture, indexed by classification value via texelFetch. */
export function buildLulcColormapTexture(device: Device): Texture {
  const data = new Uint8Array(256 * 4); // zero-initialized -> transparent by default
  for (const { value, color } of LULC_CLASSES) {
    data[value * 4 + 0] = color[0];
    data[value * 4 + 1] = color[1];
    data[value * 4 + 2] = color[2];
    data[value * 4 + 3] = 255;
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
 * Samples an integer-typed source texture and introduces an `ivec4 icolor`
 * function-local for downstream integer-aware modules to consume. Ported
 * from the deck.gl-raster land-cover example's gpu-modules/create-texture-uint.ts.
 */
const CreateTextureUint = {
  name: "create-texture-uint",
  inject: {
    "fs:#decl": `uniform highp usampler2D textureName;`,
    "fs:DECKGL_FILTER_COLOR": /* glsl */ `
      ivec4 icolor = ivec4(texture(textureName, geometry.uv));
    `,
  },
  getUniforms: (props: { textureName?: Texture }) => ({
    textureName: props.textureName,
  }),
} as const satisfies ShaderModule<{ textureName: Texture }>;

/**
 * Resolves the integer category code into a final RGBA color via
 * texelFetch (not bilinear-filtered `texture()`), so category edges stay
 * crisp instead of blending into neighboring categories. Ported from the
 * land-cover example's gpu-modules/palette-colormap.ts.
 */
const PaletteColormap = {
  name: "palette-colormap",
  inject: {
    "fs:#decl": `uniform sampler2D colormapTexture;`,
    "fs:DECKGL_FILTER_COLOR": /* glsl */ `
      color = texelFetch(colormapTexture, ivec2(icolor.r, 0), 0);
      if (color.a == 0.0) {
        discard;
      }
    `,
  },
  getUniforms: (props: { colormapTexture?: Texture }) => ({
    colormapTexture: props.colormapTexture,
  }),
} as const satisfies ShaderModule<{ colormapTexture: Texture }>;

export type LulcTileData = {
  texture: Texture;
  width: number;
  height: number;
  byteLength: number;
};

/**
 * Uploads the classification tile as `r8uint` (not `r8unorm`) so the
 * integer-aware shader modules above can read exact category codes.
 * Integer textures can only use nearest-neighbor filtering (a hard
 * WebGL2/GLES3 restriction), which is exactly what we want here — no
 * blending across category boundaries, unlike geomad's r16unorm bands.
 */
export async function getLulcTileData(
  image: GeoTIFF | Overview,
  options: GetTileDataOptions,
): Promise<LulcTileData> {
  const { device, x, y, signal } = options;
  const tile = await image.fetchTile(x, y, { signal, boundless: false });
  const { array } = tile;
  if (array.layout === "band-separate") {
    throw new Error("lulc classification data is pixel interleaved");
  }
  const { data, width, height } = array;
  const texture = device.createTexture({
    data,
    format: "r8uint",
    width,
    height,
    sampler: { minFilter: "nearest", magFilter: "nearest" },
  });
  return { texture, width, height, byteLength: data.byteLength };
}

export function makeLulcRenderTile(colormapTexture: Texture) {
  return (data: LulcTileData): RenderTileResult => ({
    renderPipeline: [
      { module: CreateTextureUint, props: { textureName: data.texture } },
      { module: PaletteColormap, props: { colormapTexture } },
    ],
  });
}
