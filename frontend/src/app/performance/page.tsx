"use client";

import { useState } from "react";
import { useApi } from "@/lib/hooks";
import { request } from "@/lib/api";
import { PlotChart } from "@/components/PlotChart";
import { ChartCard } from "@/components/QuestionChart";
import { QUESTION_CHARTS } from "@/lib/charts";
import { Async, Button, Card, DataTable, PageHeader, SectionTitle, StatCard, type Column } from "@/components/ui";
import type { PerformanceOverview } from "@/lib/types";
import { PALETTE, fmtInt, fmtNum, fmtSeconds } from "@/lib/format";

type B = Record<string, any>;
const mb = (b: number) => b / 1048576;
const find = (rows: B[] | undefined, key: string, sub: string) => rows?.find((r) => String(r[key]).includes(sub));

const PROBES = [
  { label: "GET /sensors?limit=50", path: "/api/v1/sensors?limit=50" },
  { label: "GET /hotspots?limit=100", path: "/api/v1/hotspots?limit=100" },
  { label: "GET /predictions/latest", path: "/api/v1/predictions/latest?limit=100" },
  { label: "GET /predictions/overview", path: "/api/v1/predictions/overview" },
  { label: "GET /analytics/questions/q5", path: "/api/v1/analytics/questions/q5" },
  { label: "GET /analytics table (500 rows)", path: "/api/v1/analytics/questions/q7/tables/edges?limit=500" },
  { label: "GET /models/evaluations", path: "/api/v1/models/evaluations" },
  { label: "GET /settings", path: "/api/v1/settings" },
];

interface ProbeRow { label: string; n: number; p50: number; p95: number; max: number; kb: number }

function ApiProbe() {
  const [rows, setRows] = useState<ProbeRow[]>([]);
  const [busy, setBusy] = useState(false);
  async function run() {
    setBusy(true);
    const out: ProbeRow[] = [];
    for (const p of PROBES) {
      const ms: number[] = [];
      let kb = 0;
      for (let i = 0; i < 15; i++) {
        const r = await request<unknown>(p.path);
        ms.push(r.ms);
        if (i === 0) kb = JSON.stringify(r.data).length / 1024;
      }
      ms.sort((a, b) => a - b);
      out.push({ label: p.label, n: ms.length, p50: ms[Math.floor(ms.length / 2)], p95: ms[Math.ceil(ms.length * 0.95) - 1], max: ms[ms.length - 1], kb });
      setRows([...out]);
    }
    setBusy(false);
  }
  const cols: Column<ProbeRow>[] = [
    { key: "label", header: "Endpoint (browser → Next.js proxy → FastAPI → MongoDB)" },
    { key: "n", header: "Calls", align: "right" },
    { key: "p50", header: "p50 ms", align: "right", sort: (r) => r.p50, render: (r) => fmtNum(r.p50, 1) },
    { key: "p95", header: "p95 ms", align: "right", sort: (r) => r.p95, render: (r) => fmtNum(r.p95, 1) },
    { key: "max", header: "max ms", align: "right", render: (r) => fmtNum(r.max, 1) },
    { key: "kb", header: "Payload KB", align: "right", render: (r) => fmtNum(r.kb, 1) },
  ];
  return (
    <Card>
      <SectionTitle title="Live API latency probe" hint="Measured now in your browser: 15 sequential calls per endpoint through the whole stack." right={<Button onClick={run} disabled={busy}>{busy ? "Measuring…" : "Run probe"}</Button>} />
      {rows.length ? (
        <div className="grid gap-4 lg:grid-cols-2">
          <DataTable columns={cols} rows={rows} rowKey={(r) => r.label} dense />
          <PlotChart height={280} title="Latency by endpoint" testId="probe-chart"
            data={[{ type: "bar", name: "p50", x: rows.map((r) => r.p50), y: rows.map((r) => r.label.replace("GET ", "")), orientation: "h", marker: { color: PALETTE[0] } }, { type: "bar", name: "p95", x: rows.map((r) => r.p95), y: rows.map((r) => r.label.replace("GET ", "")), orientation: "h", marker: { color: PALETTE[2] } }]}
            layout={{ barmode: "group", yaxis: { autorange: "reversed", type: "category" }, margin: { l: 190 }, xaxis: { title: { text: "ms" } } }} />
        </div>
      ) : <p className="text-sm text-muted">Press “Run probe” to measure the end-to-end response time of the main read endpoints.</p>}
    </Card>
  );
}

export default function Page() {
  const res = useApi<PerformanceOverview>("/api/v1/performance/overview");
  return (
    <>
      <PageHeader eyebrow="Apache Spark 3.5 · single node" title="Spark Performance" subtitle="Benchmarks measured on a 12-core, 7.6 GB laptop with the full 16.9 M-row dataset: storage formats, partitioning, caching and joins (Phase 4), streaming throughput and latency (Phase 6/8), job durations and model training. Everything is stored in MongoDB and served by the API." />
      <Async res={res} height={400}>{(d) => {
        const sb = d.spark_benchmarks, st = sb.storage ?? {}, pa = sb.partition ?? {}, op = sb.optimization ?? {};
        const csv = find(st.full_scan, "case", "csv_explicit"), pq = find(st.full_scan, "case", "parquet_snappy") ?? find(st.full_scan, "case", "parquet");
        const noCache = op.caching?.find((c: B) => c.strategy === "none"), cached = op.caching?.find((c: B) => c.strategy !== "none" && c.reuse_actions_s != null && c.is_cached);
        const down: B[] = pa.down?.rows ?? [];
        const coal = down.find((r) => String(r.case).includes("coalesce")), rep = down.find((r) => String(r.case).includes("repartition"));
        const env = st.environment ?? pa.environment ?? {};
        const totalAnalytics = d.analytics_jobs.reduce((s, j) => s + (j.seconds ?? 0), 0);
        const cands: B[] = d.ml_training?.summary.candidates_table ?? [];
        return (
          <div className="space-y-5">
            <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" aria-label="Headline numbers">
              <StatCard label="Parquet vs CSV full scan" value={csv && pq ? `${fmtNum(csv.seconds_median / pq.seconds_median, 1)}× faster` : "–"} sub={csv && pq ? `${fmtNum(pq.seconds_median, 2)} s vs ${fmtNum(csv.seconds_median, 1)} s (explicit schema)` : ""} icon="⚡" />
              <StatCard label="Caching a reused DataFrame" value={noCache && cached ? `${fmtNum(noCache.reuse_actions_s / Math.max(cached.reuse_actions_s, 0.01), 0)}× faster` : "–"} sub={noCache && cached ? `reuse ${fmtNum(cached.reuse_actions_s, 2)} s vs ${fmtNum(noCache.reuse_actions_s, 1)} s uncached` : ""} icon="◆" tone="teal" />
              <StatCard label="coalesce vs repartition (200→12)" value={coal && rep ? `${fmtNum(rep.seconds_median / coal.seconds_median, 1)}× faster` : "–"} sub={coal && rep ? `shuffle ${fmtNum(mb(coal.shuffleWriteBytes), 0)} MB vs ${fmtNum(mb(rep.shuffleWriteBytes), 0)} MB` : ""} icon="⇄" tone="amber" />
              <StatCard label="18 analytics jobs" value={fmtSeconds(totalAnalytics)} sub={`slowest ${[...d.analytics_jobs].sort((a, b) => (b.seconds ?? 0) - (a.seconds ?? 0))[0]?.qid.toUpperCase()}`} icon="Σ" tone="green" />
            </section>

            <div className="grid gap-5 lg:grid-cols-2">
              <Card>
                <PlotChart title="Storage format: full-scan time (s)" height={300} testId="perf-scan"
                  data={[{ type: "bar", x: (st.full_scan ?? []).map((r: B) => String(r.case).replace("scan_", "")), y: (st.full_scan ?? []).map((r: B) => r.seconds_median), marker: { color: PALETTE[0] }, text: (st.full_scan ?? []).map((r: B) => fmtNum(r.seconds_median, 1)), textposition: "outside" }]}
                  layout={{ yaxis: { title: { text: "seconds (median)" }, type: "log" }, xaxis: { type: "category" }, showlegend: false }} />
                <p className="mt-1 text-[11px] text-slate-400">log scale · source: spark_features/storage_benchmarks</p>
              </Card>
              <Card>
                <PlotChart title="Storage format: size on disk (MB)" height={300} testId="perf-size"
                  data={[{ type: "bar", x: (st.writes ?? []).map((r: B) => String(r.case).replace("write_", "")), y: (st.writes ?? []).map((r: B) => mb(r.outputBytes)), marker: { color: PALETTE[1] }, text: (st.writes ?? []).map((r: B) => fmtInt(mb(r.outputBytes))), textposition: "outside" }]}
                  layout={{ yaxis: { title: { text: "MB" } }, xaxis: { type: "category" }, showlegend: false }} />
              </Card>
              <Card>
                <PlotChart title="Shuffle partitions sweep (aggregation, AQE off)" height={300} testId="perf-shuffle"
                  data={[{ type: "scatter", mode: "lines+markers", x: (pa.shuffle_sweep ?? []).map((r: B) => Number(String(r.case).match(/partitions_(\d+)/)?.[1])), y: (pa.shuffle_sweep ?? []).map((r: B) => r.seconds_median), line: { color: PALETTE[2], width: 2 }, hovertemplate: "%{x} partitions: %{y:.2f}s<extra></extra>" }]}
                  layout={{ xaxis: { title: { text: "spark.sql.shuffle.partitions" }, type: "category" }, yaxis: { title: { text: "seconds" } }, showlegend: false }} />
              </Card>
              <Card>
                <PlotChart title="Caching strategies: first action + reuse (s)" height={300} testId="perf-cache"
                  data={[{ type: "bar", name: "first action", x: (op.caching ?? []).map((r: B) => `${r.strategy}${r.storage_level && r.storage_level !== "-" ? "" : ""}`), y: (op.caching ?? []).map((r: B) => r.first_action_s), marker: { color: PALETTE[0] } },
                    { type: "bar", name: "reuse (2 actions)", x: (op.caching ?? []).map((r: B) => `${r.strategy}`), y: (op.caching ?? []).map((r: B) => r.reuse_actions_s), marker: { color: PALETTE[3] } }]}
                  layout={{ barmode: "stack", xaxis: { type: "category" }, yaxis: { title: { text: "seconds" } } }} />
              </Card>
              <Card>
                <PlotChart title="Join strategies (s)" height={280} testId="perf-joins"
                  data={[{ type: "bar", x: (op.joins ?? []).map((r: B) => String(r.case).replace("join_", "")), y: (op.joins ?? []).map((r: B) => r.seconds_median), marker: { color: PALETTE[4] }, text: (op.joins ?? []).map((r: B) => fmtNum(r.seconds_median, 2)), textposition: "outside" }]}
                  layout={{ xaxis: { type: "category" }, yaxis: { title: { text: "seconds" } }, showlegend: false }} />
              </Card>
              <Card>
                <PlotChart title="Analytics job duration per question (s)" height={280} testId="perf-jobs"
                  data={[{ type: "bar", x: d.analytics_jobs.map((j) => j.qid.toUpperCase()), y: d.analytics_jobs.map((j) => j.seconds), marker: { color: d.analytics_jobs.map((j) => (j.status === "succeeded" ? PALETTE[0] : "#d9534f")) } }]}
                  layout={{ xaxis: { type: "category" }, yaxis: { title: { text: "seconds" }, type: "log" }, showlegend: false }} />
              </Card>
              {cands.length > 0 && (
                <Card className="lg:col-span-2">
                  <PlotChart title="MLlib training time per hyper-parameter candidate (s)" height={300} testId="perf-train"
                    data={[{ type: "bar", x: cands.map((c) => `${String(c.algorithm).replace("_", " ")} ${Object.values(JSON.parse(c.params)).join("/")}`), y: cands.map((c) => c.train_s), marker: { color: cands.map((c) => (c.selected ? PALETTE[0] : "#c9c4ee")) }, text: cands.map((c) => fmtNum(c.train_s, 0)), textposition: "outside" }]}
                    layout={{ xaxis: { type: "category", tickangle: -25 }, yaxis: { title: { text: "seconds (8% sample, 890 k rows)" } }, showlegend: false }} />
                </Card>
              )}
            </div>

            <SectionTitle title="Streaming throughput and latency" hint="Q15 benchmark (file-source replay) and the Kafka run's progress metrics." />
            <div className="grid gap-5 lg:grid-cols-3">
              {(QUESTION_CHARTS.q15 ?? []).map((s) => <ChartCard key={s.id} qid="q15" spec={s} />)}
            </div>
            <Card>
              <SectionTitle title="Kafka → Spark Structured Streaming runs" hint="From stream_metrics (StreamingQueryListener): micro-batch duration per query." />
              <DataTable dense rowKey={(r) => `${r.run}|${r.query}`} rows={d.streaming.summary} columns={[
                { key: "run", header: "Run" }, { key: "query", header: "Query" }, { key: "b", header: "Batches", align: "right", render: (r) => fmtInt(r.batches_with_data) }, { key: "rows", header: "Input rows", align: "right", render: (r) => fmtInt(r.input_rows) },
                { key: "p50", header: "Batch p50 ms", align: "right", sort: (r) => r.batch_ms_p50, render: (r) => fmtInt(r.batch_ms_p50) }, { key: "p95", header: "p95 ms", align: "right", sort: (r) => r.batch_ms_p95, render: (r) => fmtInt(r.batch_ms_p95) },
                { key: "rps", header: "Rows/s (mean)", align: "right", render: (r) => fmtNum(r.mean_processed_rows_per_s, 0) }]} maxHeight={320} />
            </Card>

            <ApiProbe />

            <Card>
              <SectionTitle title="Benchmark findings (generated by the Spark jobs)" hint={`Spark ${env.spark_version ?? "3.5"} · master ${env.master ?? "local[*]"} · ${env.cpu_count ?? 12} cores · ${env.ram_gb ?? 7.6} GB RAM · driver ${env.driver_memory ?? "3g"}`} />
              <div className="grid gap-4 md:grid-cols-3">
                {(["storage", "partition", "optimization"] as const).map((k) => (
                  <div key={k}><p className="mb-1 text-xs font-semibold uppercase tracking-wide text-lav-700">{sb[k]?.title ?? k}</p>
                    <ul className="space-y-1.5 text-sm text-muted">{(sb[k]?.headline ?? []).map((h: string, i: number) => <li key={i} className="flex gap-2"><span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-lav-500" />{h}</li>)}</ul></div>
                ))}
              </div>
            </Card>
          </div>
        );
      }}</Async>
    </>
  );
}
