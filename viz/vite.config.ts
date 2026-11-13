import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  // Served from https://auspatious.github.io/ldn-lulc/ on GitHub Pages.
  base: "/ldn-lulc/",
  worker: { format: "es" },
  server: {
    port: 3000,
  },
});
