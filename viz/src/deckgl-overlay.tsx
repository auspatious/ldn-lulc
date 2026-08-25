import { MapboxOverlay, type MapboxOverlayProps } from "@deck.gl/mapbox";
import { useControl } from "react-map-gl/maplibre";

/** Renders deck.gl layers on top of the maplibre map. */
export function DeckGLOverlay(props: MapboxOverlayProps) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay(props));
  overlay.setProps(props);
  return null;
}
