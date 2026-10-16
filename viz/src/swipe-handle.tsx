import type { PointerEvent as ReactPointerEvent } from "react";

export interface SwipeHandleProps {
  /** Handle position as a fraction of the window width, 0 to 1. */
  split: number;
  onChange: (split: number) => void;
}

/** Vertical draggable divider for the compare view. */
export function SwipeHandle({ split, onChange }: SwipeHandleProps) {
  /** Track the pointer on the window until release. */
  function handlePointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    event.preventDefault();
    const onMove = (e: PointerEvent) =>
      onChange(Math.min(Math.max(e.clientX / window.innerWidth, 0), 1));
    const onUp = () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  }

  return (
    <div
      onPointerDown={handlePointerDown}
      role="separator"
      aria-orientation="vertical"
      aria-label="Compare divider"
      style={{
        position: "absolute",
        top: 0,
        bottom: 0,
        left: `calc(${split * 100}% - 12px)`,
        width: 24,
        cursor: "ew-resize",
        touchAction: "none",
        zIndex: 10,
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <div
        style={{
          position: "absolute",
          top: 0,
          bottom: 0,
          width: 2,
          background: "white",
          boxShadow: "0 0 4px rgba(0, 0, 0, 0.4)",
        }}
      />
      <div
        style={{
          position: "relative",
          width: 24,
          height: 24,
          borderRadius: "50%",
          background: "white",
          border: "1px solid rgba(0, 0, 0, 0.2)",
          boxShadow: "0 2px 8px rgba(0, 0, 0, 0.3)",
        }}
      />
    </div>
  );
}
