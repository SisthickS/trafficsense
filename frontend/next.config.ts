import type { NextConfig } from "next";

// The dashboard talks to the FastAPI service only through the `/backend/*` route handler (src/app/backend),
// so the browser never needs CORS and an optional API key stays on the server.
const config: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  // node_modules and .next live on the Linux filesystem (symlinks) because the Windows drive is slow under WSL.
  turbopack: { root: "/" },
};

export default config;
