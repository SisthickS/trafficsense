import { describe, expect, it } from "vitest";
import { ApiError, qs, request } from "@/lib/api";
import { fmtNum, fmtInt } from "@/lib/format";

const ID = 9900002;
const sensor = { sensor_id: ID, latitude: 37.5, longitude: -122.1, name: "vitest sensor", status: "active", tags: ["vitest"] };

describe("client helpers", () => {
  it("builds query strings and skips empty values", () => {
    expect(qs({ a: 1, b: "", c: null, d: undefined, e: "x y" })).toBe("?a=1&e=x+y");
    expect(qs({})).toBe("");
  });
  it("formats numbers", () => {
    expect(fmtInt(1234567)).toBe("1,234,567");
    expect(fmtNum(0.98765, 3)).toBe("0.988");
  });
});

describe("API client against the live backend", () => {
  it("reads the readiness probe and the status overview", async () => {
    const ready = await request<{ status: string }>("/ready");
    expect(ready.data.status).toBe("ready");
    const s = await request<{ counts: Record<string, number> }>("/api/v1/status");
    expect(s.data.counts.sensor_metadata).toBeGreaterThan(300);
    expect(s.data.counts.analysis_results).toBeGreaterThanOrEqual(95);
  });

  it("turns error envelopes into ApiError", async () => {
    const e = await request("/api/v1/sensors/123456789").catch((x) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect(e.status).toBe(404);
    expect(e.code).toBe("not_found");
    expect(e.requestId).toBeTruthy();
    const v = await request("/api/v1/sensors", { method: "POST", json: { sensor_id: -5 } }).catch((x) => x);
    expect(v.status).toBe(422);
    expect(v.details.length).toBeGreaterThan(0);
    expect(v.readable).toMatch(/sensor_id|latitude/);
  });

  it("persists create → patch (If-Match) → stale write rejected → delete", async () => {
    await request(`/api/v1/sensors/${ID}`, { method: "DELETE" }).catch(() => undefined);
    const c = await request<{ version: number }>("/api/v1/sensors", { method: "POST", json: sensor });
    expect(c.status).toBe(201);
    expect(c.data.version).toBe(1);

    const p = await request<{ version: number; name: string }>(`/api/v1/sensors/${ID}`, { method: "PATCH", json: { name: "renamed" }, ifMatch: 1 });
    expect(p.data).toMatchObject({ version: 2, name: "renamed" });

    const stale = await request(`/api/v1/sensors/${ID}`, { method: "PATCH", json: { name: "lost update" }, ifMatch: 1 }).catch((x) => x);
    expect(stale.status).toBe(412);

    const back = await request<{ name: string; version: number }>(`/api/v1/sensors/${ID}`);
    expect(back.data).toMatchObject({ name: "renamed", version: 2 });

    expect((await request(`/api/v1/sensors/${ID}`, { method: "DELETE" })).status).toBe(204);
    const gone = await request(`/api/v1/sensors/${ID}`).catch((x) => x);
    expect(gone.status).toBe(404);
  });

  it("stores and restores an analyst setting", async () => {
    const before = await request<{ value: number; is_default: boolean }>("/api/v1/settings/watchlist_size");
    const next = before.data.value === 11 ? 12 : 11;
    const put = await request<{ value: number }>("/api/v1/settings/watchlist_size", { method: "PUT", json: { value: next } });
    expect(put.data.value).toBe(next);
    expect((await request<{ value: number; is_default: boolean }>("/api/v1/settings/watchlist_size")).data).toMatchObject({ value: next, is_default: false });
    const bad = await request("/api/v1/settings/watchlist_size", { method: "PUT", json: { value: "not a number" } }).catch((x) => x);
    expect(bad.status).toBe(422);
    if (before.data.is_default) expect((await request("/api/v1/settings/watchlist_size", { method: "DELETE" })).status).toBe(204);
    else await request("/api/v1/settings/watchlist_size", { method: "PUT", json: { value: before.data.value } });
    expect((await request<{ value: number }>("/api/v1/settings/watchlist_size")).data.value).toBe(before.data.value);
  });
});
