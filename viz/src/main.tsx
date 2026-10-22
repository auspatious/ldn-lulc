import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.js";
import { ExampleProvider } from "./shared/index.js";

const root = document.getElementById("root");
if (!root) {
  throw new Error("Missing #root element");
}

createRoot(root).render(
  <StrictMode>
    <ExampleProvider>
      <App />
    </ExampleProvider>
  </StrictMode>,
);
