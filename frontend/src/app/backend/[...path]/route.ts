import type { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const API_URL = (process.env.TRAFFICSENSE_API_URL ?? "http://127.0.0.1:8000").replace(/\/+$/, "");
const API_KEY = process.env.TRAFFICSENSE_API_KEY ?? "";            // server-side only: never shipped to the browser

const FORWARD_REQUEST = ["content-type", "accept", "if-match", "x-updated-by", "x-request-id"];
const FORWARD_RESPONSE = ["content-type", "etag", "location", "cache-control", "retry-after", "x-request-id", "content-length"];

async function forward(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }): Promise<Response> {
  const { path } = await ctx.params;
  const target = `${API_URL}/${path.map(encodeURIComponent).join("/")}${req.nextUrl.search}`;
  const headers = new Headers();
  for (const h of FORWARD_REQUEST) {
    const v = req.headers.get(h);
    if (v) headers.set(h, v);
  }
  if (API_KEY) headers.set("x-api-key", API_KEY);
  const hasBody = !["GET", "HEAD"].includes(req.method);
  try {
    const upstream = await fetch(target, { method: req.method, headers, body: hasBody ? await req.arrayBuffer() : undefined, cache: "no-store", redirect: "manual" });
    const out = new Headers();
    for (const h of FORWARD_RESPONSE) {
      const v = upstream.headers.get(h);
      if (v) out.set(h, v);
    }
    return new Response(upstream.status === 204 ? null : upstream.body, { status: upstream.status, headers: out });
  } catch {
    // FastAPI is down: answer in the API's own error format so the UI can show it like any other error
    return Response.json({ error: { code: "api_unreachable", message: `The API at ${API_URL} is not reachable. Start it with: uvicorn src.api.main:app` } }, { status: 502 });
  }
}

export { forward as GET, forward as POST, forward as PUT, forward as PATCH, forward as DELETE };
