import type { Layer, Viewport } from "@deck.gl/core";
import { LayerExtension } from "@deck.gl/core";
import type { ShaderModule } from "@luma.gl/shadertools";

type SplitClipUniforms = {
  /** Divider position in device pixels from the canvas's left edge. */
  splitX: number;
  /** -1 keeps pixels left of the divider, 1 keeps pixels right of it. */
  side: number;
};

const splitClip = {
  name: "splitClip",
  fs: /* glsl */ `
    uniform splitClipUniforms {
      float splitX;
      float side;
    } splitClip;
  `,
  inject: {
    "fs:DECKGL_FILTER_COLOR": /* glsl */ `
      if ((gl_FragCoord.x - splitClip.splitX) * splitClip.side < 0.0) {
        discard;
      }
    `,
  },
  uniformTypes: { splitX: "f32", side: "f32" },
} as const satisfies ShaderModule<SplitClipUniforms>;

export type SplitClipProps = {
  /** Divider position as a fraction of the viewport width, 0 to 1. */
  splitPosition: number;
};

/**
 * Clip a layer to one side of a vertical screen-space divider, for swipe
 * comparison on a single map. Works at any bearing or pitch.
 */
export class SplitClipExtension extends LayerExtension<{
  side: "left" | "right";
}> {
  static extensionName = "SplitClipExtension";
  static defaultProps = { splitPosition: 0.5 };

  /** Add the clip shader module. */
  getShaders() {
    return { modules: [splitClip] };
  }

  /** Convert the divider position to device pixels for this frame. */
  draw(
    this: Layer<SplitClipProps>,
    {
      shaderModuleProps,
    }: {
      shaderModuleProps: {
        project: { viewport: Viewport; devicePixelRatio: number };
      };
    },
    extension: SplitClipExtension,
  ) {
    const { viewport, devicePixelRatio } = shaderModuleProps.project;
    this.setShaderModuleProps({
      splitClip: {
        splitX: this.props.splitPosition * viewport.width * devicePixelRatio,
        side: extension.opts.side === "left" ? -1 : 1,
      },
    });
  }
}
