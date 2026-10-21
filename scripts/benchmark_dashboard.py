"""Dashboard / API performance benchmark: endpoint latency (direct and through the Next.js proxy), page response time,
concurrent throughput and JS bundle size. Writes reports/final/dashboard_benchmark.json.

Run (API on :8000 and the production dashboard on :3000 must be up):
    python scripts/benchmark_dashboard.py [--api http://127.0.0.1:8000] [--web http://127.0.0.1:3000] [--n 30]
"""
from __future__ import annotations

import argparse
import gzip
import json
import statistics as st
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "final" / "dashboard_benchmark.json"
ENDPOINTS = ["/ready", "/api/v1/status", "/api/v1/sensors?limit=50", "/api/v1/hotspots?limit=50", "/api/v1/predictions/latest?limit=50",
             "/api/v1/analytics/questions/q5", "/api/v1/analytics/questions/q7/tables/edges?limit=500", "/api/v1/performance/overview",
             "/api/v1/status/database"]
PAGES = ["/", "/analytics", "/hotspots", "/live", "/models", "/performance", "/data", "/status"]


def pct(xs: list[float], p: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, round(p / 100 * (len(s) - 1)))]


def timed(client: httpx.Client, url: str, n: int) -> dict:
    for _ in range(3):
        client.get(url)                                    # warm-up (connection, caches)
    ms, size = [], 0
    for _ in range(n):
        t0 = time.perf_counter()
        r = client.get(url)
        ms.append((time.perf_counter() - t0) * 1000)
        r.raise_for_status()
        size = len(r.content)
    return {"p50_ms": round(pct(ms, 50), 1), "p95_ms": round(pct(ms, 95), 1), "mean_ms": round(st.fmean(ms), 1), "max_ms": round(max(ms), 1), "bytes": size}


def throughput(url: str, workers: int, per_worker: int) -> dict:
    def work(_: int) -> list[float]:
        out = []
        with httpx.Client(timeout=30) as c:
            for _ in range(per_worker):
                t0 = time.perf_counter()
                c.get(url).raise_for_status()
                out.append((time.perf_counter() - t0) * 1000)
        return out
    t0 = time.perf_counter()
    with ThreadPoolExecutor(workers) as ex:
        lat = [x for r in ex.map(work, range(workers)) for x in r]
    wall = time.perf_counter() - t0
    return {"workers": workers, "requests": len(lat), "rps": round(len(lat) / wall, 1), "p50_ms": round(pct(lat, 50), 1), "p95_ms": round(pct(lat, 95), 1)}


def bundle() -> dict:
    chunks = list((ROOT / "frontend" / ".next" / "static").rglob("*.js"))
    if not chunks:
        return {}
    raw = sum(f.stat().st_size for f in chunks)
    gz = sum(len(gzip.compress(f.read_bytes(), 6)) for f in chunks)
    biggest = sorted(chunks, key=lambda f: f.stat().st_size, reverse=True)[:3]
    return {"js_files": len(chunks), "js_total_kb": round(raw / 1024), "js_gzip_kb": round(gz / 1024),
            "largest": [{"file": f.name, "kb": round(f.stat().st_size / 1024)} for f in biggest]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8000")
    ap.add_argument("--web", default="http://127.0.0.1:3000")
    ap.add_argument("--n", type=int, default=30)
    a = ap.parse_args()
    result: dict = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "samples_per_endpoint": a.n, "endpoints": {}, "pages": {}}
    with httpx.Client(timeout=30) as c:
        for ep in ENDPOINTS:
            result["endpoints"][ep] = {"direct": timed(c, a.api + ep, a.n), "via_proxy": timed(c, a.web + "/backend" + ep, a.n)}
            d, v = result["endpoints"][ep]["direct"], result["endpoints"][ep]["via_proxy"]
            print(f"{ep:<62} direct p50 {d['p50_ms']:>7.1f} ms  proxy p50 {v['p50_ms']:>7.1f} ms  {d['bytes'] / 1024:>7.1f} KB")
        for p in PAGES:
            result["pages"][p] = timed(c, a.web + p, a.n)
            print(f"page {p:<14} p50 {result['pages'][p]['p50_ms']:>6.1f} ms  html {result['pages'][p]['bytes'] / 1024:.1f} KB")
    result["throughput_hotspots"] = throughput(a.api + "/api/v1/hotspots?limit=50", 8, 25)
    result["throughput_via_proxy"] = throughput(a.web + "/backend/api/v1/hotspots?limit=50", 8, 25)
    result["bundle"] = bundle()
    print("throughput direct:", result["throughput_hotspots"])
    print("throughput proxy :", result["throughput_via_proxy"])
    print("bundle:", result["bundle"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print("wrote", OUT.relative_to(ROOT))


if __name__ == "__main__":
    main()
