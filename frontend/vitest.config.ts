import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "node:path";

// The tests talk to the REAL FastAPI + MongoDB stack (no mocked data); only Plotly (needs a canvas) is stubbed.
const API = process.env.TRAFFICSENSE_TEST_API ?? "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": path.resolve(__dirname, "src") } },
  test: {
    environment: "jsdom",
    include: ["tests/**/*.test.{ts,tsx}"],
    setupFiles: ["tests/setup.ts"],
    env: { NEXT_PUBLIC_API_BASE: API },
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
