declare module "@maplibre/maplibre-gl-compare" {
  import type { Map as MaplibreMap } from "maplibre-gl";

  export default class Compare {
    constructor(
      mapA: MaplibreMap,
      mapB: MaplibreMap,
      container: string | HTMLElement,
      options?: { orientation?: "vertical" | "horizontal"; mousemove?: boolean },
    );
    setSlider(x: number): void;
    on(type: "slideend", fn: (e: { currentPosition: number }) => void): this;
    off(type: "slideend", fn: (e: { currentPosition: number }) => void): this;
    remove(): void;
  }
}
