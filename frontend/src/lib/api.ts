/** Thin fetch client for the FastAPI backend (through the `/backend` proxy route). Every non-2xx answer becomes an ApiError. */

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "/backend";

export interface ErrorDetail { field?: string; message?: string; type?: string; expected_version?: number; current_version?: number }

export class ApiError extends Error {
  status: number;
  code: string;
  details: ErrorDetail[];
  requestId?: string;
  constructor(status: number, code: string, message: string, details: ErrorDetail[] = [], requestId?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.details = details;
    this.requestId = requestId;
  }
  /** One readable line, including per-field validation messages. */
  get readable(): string {
    const fields = this.details.filter((d) => d.field).map((d) => `${d.field?.replace(/^(body|query|path)\./, "")}: ${d.message}`);
    return fields.length ? `${this.message} (${fields.join("; ")})` : this.message;
  }
}

export interface Reply<T> { data: T; status: number; etag: number | null; location: string | null; ms: number }

export interface RequestOptions {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  json?: unknown;
  /** Version the caller last saw: sent as `If-Match`; the server answers 412 if the document changed meanwhile. */
  ifMatch?: number | null;
  actor?: string;
  signal?: AbortSignal;
}

export async function request<T = unknown>(path: string, opts: RequestOptions = {}): Promise<Reply<T>> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (opts.json !== undefined) headers["Content-Type"] = "application/json";
  if (opts.ifMatch != null) headers["If-Match"] = `"${opts.ifMatch}"`;
  if (opts.actor) headers["X-Updated-By"] = opts.actor;
  const t0 = typeof performance !== "undefined" ? performance.now() : Date.now();
  const res = await fetch(`${API_BASE}${path}`, {
    method: opts.method ?? "GET",
    headers,
    body: opts.json !== undefined ? JSON.stringify(opts.json) : undefined,
    signal: opts.signal,
    cache: "no-store",
  });
  const ms = (typeof performance !== "undefined" ? performance.now() : Date.now()) - t0;
  if (!res.ok) {
    let body: any = null;
    try { body = await res.json(); } catch { /* not JSON */ }
    const e = body?.error;
    throw new ApiError(res.status, e?.code ?? "http_error", e?.message ?? `${res.status} ${res.statusText}`, e?.details ?? [], e?.request_id);
  }
  const etag = res.headers.get("ETag")?.match(/\d+/)?.[0];
  const data = (res.status === 204 ? undefined : await res.json()) as T;
  return { data, status: res.status, etag: etag ? Number(etag) : null, location: res.headers.get("Location"), ms };
}

export const fetcher = async <T,>(path: string): Promise<T> => (await request<T>(path)).data;

export function qs(params: Record<string, string | number | boolean | null | undefined>): string {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== "") p.set(k, String(v));
  const s = p.toString();
  return s ? `?${s}` : "";
}

export const figureUrl = (name: string) => `${API_BASE}/api/v1/analytics/figures/${name}`;
