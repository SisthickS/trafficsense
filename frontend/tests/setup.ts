import "@testing-library/jest-dom/vitest";
import { afterEach, vi } from "vitest";
import { cleanup } from "@testing-library/react";
import React from "react";

afterEach(() => cleanup());

// Plotly needs a real canvas/SVG engine; replace it by a marker that exposes what would be drawn.
vi.mock("@/components/PlotChart", () => ({
  PlotChart: ({ title, data, testId }: { title?: string; data: unknown[]; testId?: string }) =>
    React.createElement("div", { "data-testid": testId ?? "plot", "data-traces": data.length }, title),
}));

if (!window.matchMedia) {
  window.matchMedia = ((q: string) => ({ matches: false, media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {}, onchange: null, dispatchEvent: () => false })) as unknown as typeof window.matchMedia;
}
class RO { observe() {} unobserve() {} disconnect() {} }
(globalThis as unknown as { ResizeObserver: typeof RO }).ResizeObserver ??= RO;
