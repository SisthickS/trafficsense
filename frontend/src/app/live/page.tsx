"use client";

import { useEffect, useMemo, useState } from "react";
import { useSWRConfig } from "swr";
import { useApi } from "@/lib/hooks";
import { request } from "@/lib/api";
import { PlotChart } from "@/components/PlotChart";
import { Async, Badge, Button, Card, DataTable, EmptyState, ErrorState, Field, PageHeader, Pills, SectionTitle, StatCard, useToast, type Column } from "@/components/ui";
import type { Hotspot, LatestPredictions, Paged, Prediction, Setting, SlotSummary, StreamSummary } from "@/lib/types";
import { PALETTE, fmtDate, fmtInt, fmtNum, fmtPct } from "@/lib/format";

interface StreamHealth { healthy: boolean; reason?: string; seconds_since_last_commit?: number; last_batch_id?: number; dead_letters?: number; late_events?: number }

export default function Page() {
  const { notify } = useToast();
  const { mutate } = useSWRConfig();
  const [mode, setMode] = useState<"follow" | "playback">("follow");
  const [idx, setIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [draft, setDraft] = useState<number | null>(null);

  const refresh = useApi<Setting>("/api/v1/settings/dashboard_refresh_seconds");
  const everyMs = Math.max(2, Number(refresh.data?.value ?? 10)) * 1000;
  const slotsRes = useApi<{ threshold: number; slots: SlotSummary[] }>("/api/v1/predictions/slots", { refreshMs: mode === "follow" ? everyMs : undefined });
  const slots = slotsRes.data?.slots ?? [];
  const cur = slots.length ? slots[mode === "follow" ? slots.length - 1 : Math.min(idx, slots.length - 1)] : null;
  const preds = useApi<LatestPredictions>(cur ? `/api/v1/predictions/latest?slot=${cur.slot}&limit=500` : null);
  const hot = useApi<Paged<Hotspot>>("/api/v1/hotspots?limit=500");
  const health = useApi<StreamHealth>("/api/v1/streaming/health?max_age_s=120", { refreshMs: everyMs });
  const summary = useApi<StreamSummary[]>("/api/v1/streaming/metrics/summary");
  const thr = slotsRes.data?.threshold ?? 0.686;

  useEffect(() => { if (mode === "playback" && slots.length && idx >= slots.length) setIdx(0); }, [mode, slots.length, idx]);
  useEffect(() => {
    if (!playing || mode !== "playback" || !slots.length) return;
    const t = setInterval(() => setIdx((i) => (i + 1 >= slots.length ? (setPlaying(false), i) : i + 1)), 1500 / speed);
    return () => clearInterval(t);
  }, [playing, mode, slots.length, speed]);

  const coords = useMemo(() => new Map((hot.data?.items ?? []).map((h) => [h.sensor_id, h])), [hot.data]);
  const items = preds.data?.items ?? [];
  const alerts = items.filter((p) => p.alert);

  async function applyThreshold() {
    if (draft == null) return;
    try {
      const r = await request<Setting>("/api/v1/settings/alert_threshold", { method: "PUT", json: { value: draft }, actor: "dashboard" });
      notify({ kind: "ok", title: `Alert threshold saved: ${draft}`, detail: `MongoDB settings document version ${r.data.version}` });
      setDraft(null);
      mutate((k) => typeof k === "string" && k.includes("/predictions/"));
    } catch (e: any) { notify({ kind: "error", title: "Could not save the threshold", detail: e.readable ?? String(e) }); }
  }

  const cols: Column<Prediction>[] = [
    { key: "sensor", header: "Sensor", render: (r) => <span className="font-semibold text-lav-800">{r.sensor_id}</span>, sort: (r) => r.sensor_id },
    { key: "p", header: "P(congested in 5 min)", align: "right", sort: (r) => r.probability, render: (r) => <span className="inline-flex items-center gap-2"><span className="h-1.5 w-16 overflow-hidden rounded-full bg-lav-100"><span className="block h-full rounded-full bg-lav-700" style={{ width: `${r.probability * 100}%` }} /></span>{fmtPct(r.probability, 1)}</span> },
    { key: "speed", header: "Speed now", align: "right", sort: (r) => r.speed_mph, render: (r) => `${fmtNum(r.speed_mph, 1)} mph` },
    { key: "alert", header: "Alert", render: (r) => <Badge tone={r.alert ? "rose" : "slate"}>{r.alert ? "alert" : "ok"}</Badge> },
    { key: "imp", header: "Gap-filled", render: (r) => (r.is_imputed ? <Badge tone="amber">forward-filled</Badge> : "–") },
  ];
  const hs = health.data;
  const latestRun = summary.data?.filter((s) => s.query === "predictions").sort((a, b) => (a.last_at < b.last_at ? 1 : -1))[0];

  return (
    <>
      <PageHeader eyebrow="Kafka · Structured Streaming · MongoDB" title="Simulated Live Traffic"
        subtitle="Replayed PEMS-BAY readings flow through Kafka and Spark Structured Streaming; the 5-minute-ahead predictions are stored in MongoDB and shown here. Follow the newest slot, or replay the stored stream slot by slot."
        right={<Pills<"follow" | "playback"> label="Mode" value={mode} onChange={(m) => { setMode(m); setPlaying(false); setIdx(0); }} options={[{ value: "follow", label: `Follow latest (every ${everyMs / 1000}s)` }, { value: "playback", label: "Playback" }]} />} />

      <section className="mb-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-5" aria-label="Current slot">
        <StatCard label="Slot (UTC)" value={cur ? cur.event_ts.slice(11, 16) : "–"} sub={cur ? cur.event_ts.slice(0, 10) : "no predictions"} icon="⏱" loading={slotsRes.isLoading} />
        <StatCard label="Sensors scored" value={fmtInt(preds.data?.total)} sub={`slot ${cur?.slot ?? "–"}`} icon="◉" tone="teal" loading={!preds.data} />
        <StatCard label="Alerts" value={fmtInt(alerts.length)} sub={`P ≥ ${fmtNum(thr, 3)}`} icon="▲" tone="rose" loading={!preds.data} />
        <StatCard label="Network mean risk" value={fmtPct(cur?.mean_probability, 1)} sub={`max ${fmtPct(cur?.max_probability, 0)}`} icon="◇" tone="amber" loading={!cur} />
        <StatCard label="Stream job" value={hs ? (hs.healthy ? "running" : "idle") : "…"} sub={hs?.healthy ? `last batch ${fmtNum(hs.seconds_since_last_commit, 0)} s ago` : hs?.reason ?? (hs?.seconds_since_last_commit != null ? `last commit ${Math.round(hs.seconds_since_last_commit / 60)} min ago (replay finished)` : "")} icon="↻" tone={hs?.healthy ? "green" : "slate"} loading={!hs} />
      </section>

      {mode === "playback" && slots.length > 0 && (
        <Card className="mb-5">
          <div className="flex flex-wrap items-center gap-4">
            <Button onClick={() => setPlaying((p) => !p)} aria-label={playing ? "Pause" : "Play"}>{playing ? "❚❚ Pause" : "▶ Play"}</Button>
            <Button variant="soft" onClick={() => { setPlaying(false); setIdx(0); }}>⏮ Restart</Button>
            <input type="range" min={0} max={slots.length - 1} value={Math.min(idx, slots.length - 1)} onChange={(e) => { setPlaying(false); setIdx(Number(e.target.value)); }} className="min-w-48 flex-1 accent-lav-700" aria-label="Slot" />
            <span className="text-sm tabular-nums text-muted" data-testid="playback-pos">{Math.min(idx, slots.length - 1) + 1} / {slots.length}</span>
            <Pills<string> label="Speed" value={String(speed)} onChange={(v) => setSpeed(Number(v))} options={[{ value: "1", label: "1×" }, { value: "2", label: "2×" }, { value: "5", label: "5×" }]} />
          </div>
        </Card>
      )}

      {slotsRes.error && !slotsRes.data ? <ErrorState error={slotsRes.error} retry={() => slotsRes.mutate()} /> : slotsRes.data && slots.length === 0 ? (
        <EmptyState>No predictions are stored yet. Run the streaming demo (<code>bash scripts/run_phase8.sh</code> or <code>bash scripts/run_phase9.sh --seed-predictions</code>) and reload.</EmptyState>
      ) : (
        <div className="grid gap-5 xl:grid-cols-5">
          <Card className="xl:col-span-3">
            {!preds.data || !hot.data ? <div className="h-[400px] animate-pulse rounded-xl bg-lav-100" /> : (
              <PlotChart title="Congestion risk per sensor (next 5 minutes)" height={420} testId="live-map"
                data={[{ type: "scatter", mode: "markers", x: items.map((p) => coords.get(p.sensor_id)?.longitude), y: items.map((p) => coords.get(p.sensor_id)?.latitude),
                  text: items.map((p) => `sensor ${p.sensor_id}<br>P ${fmtPct(p.probability, 1)} · speed ${fmtNum(p.speed_mph, 0)} mph`), hovertemplate: "%{text}<extra></extra>",
                  marker: { size: items.map((p) => (p.alert ? 14 : 8)), color: items.map((p) => p.probability), colorscale: "RdYlGn", reversescale: true, cmin: 0, cmax: 1, showscale: true, colorbar: { title: { text: "P(congested)", side: "right" }, thickness: 12, tickformat: ".0%" }, line: { width: items.map((p) => (p.alert ? 1.5 : 0.4)), color: items.map((p) => (p.alert ? "#1f1b3d" : "#fff")) }, opacity: 0.9 } }]}
                layout={{ xaxis: { title: { text: "Longitude" } }, yaxis: { title: { text: "Latitude" }, scaleanchor: "x" }, showlegend: false }} />
            )}
          </Card>
          <Card className="xl:col-span-2">
            <PlotChart title="Alerts and mean risk over the stream" height={420} testId="live-timeline"
              data={[{ type: "bar", name: "alerts", x: slots.map((s) => s.event_ts), y: slots.map((s) => s.alerts), marker: { color: slots.map((s) => (cur && s.slot === cur.slot ? "#e8836a" : PALETTE[0])) }, hovertemplate: "%{x|%H:%M}: %{y} alerts<extra></extra>" },
                { type: "scatter", mode: "lines", name: "mean risk", x: slots.map((s) => s.event_ts), y: slots.map((s) => s.mean_probability), yaxis: "y2", line: { color: PALETTE[3], width: 2 }, hovertemplate: "%{x|%H:%M}: %{y:.1%}<extra></extra>" }]}
              layout={{ yaxis: { title: { text: "sensors in alert" } }, yaxis2: { overlaying: "y", side: "right", tickformat: ".0%", title: { text: "mean P" }, showgrid: false }, xaxis: { title: { text: "time (UTC)" } } }}
              onPointClick={(p) => { const i = slots.findIndex((s) => s.event_ts === p.x || s.event_ts.startsWith(String(p.x).replace(" ", "T"))); if (i >= 0) { setMode("playback"); setPlaying(false); setIdx(i); } }} />
          </Card>
        </div>
      )}

      <div className="mt-5 grid gap-5 xl:grid-cols-5">
        <Card className="xl:col-span-3">
          <SectionTitle title="Highest-risk sensors in this slot" hint="Probabilities come from the saved Spark pipeline model; alert = probability ≥ the threshold setting." />
          <Async res={preds} height={300}>{(d) => <DataTable columns={cols} rows={d.items.slice(0, 15)} rowKey={(r) => r.sensor_id} dense initialSort={{ key: "p", dir: "desc" }} empty="No predictions in this slot." />}</Async>
        </Card>
        <div className="space-y-5 xl:col-span-2">
          <Card>
            <SectionTitle title="Alert threshold" hint="Saved to MongoDB (settings) and applied to every alert on this page." />
            <div className="flex items-center gap-3">
              <input type="range" min={0.05} max={0.95} step={0.01} value={draft ?? thr} onChange={(e) => setDraft(Number(e.target.value))} className="flex-1 accent-lav-700" aria-label="Alert threshold" />
              <span className="w-12 text-right text-sm font-semibold tabular-nums">{fmtNum(draft ?? thr, 2)}</span>
              <Button size="sm" disabled={draft == null || draft === thr} onClick={applyThreshold}>Save</Button>
            </div>
            <p className="mt-2 text-xs text-muted">Lower = more alerts, fewer missed congestions. At cost ratio 10:1 the validation-tuned value is 0.686.</p>
          </Card>
          <Card>
            <SectionTitle title="Streaming query metrics" hint={latestRun ? `run “${latestRun.run}” · from the progress listener` : undefined} />
            <Async res={summary} height={120}>{(rows) => {
              const run = rows.filter((r) => r.run === latestRun?.run);
              return run.length ? (
                <table className="w-full text-xs"><thead className="text-left text-muted"><tr><th className="py-1">query</th><th className="text-right">batches</th><th className="text-right">p50 ms</th><th className="text-right">p95 ms</th><th className="text-right">rows</th></tr></thead>
                  <tbody>{run.map((r) => <tr key={r.query} className="border-t border-lav-100"><td className="py-1.5 font-medium">{r.query}</td><td className="text-right">{fmtInt(r.batches_with_data)}</td><td className="text-right">{fmtInt(r.batch_ms_p50)}</td><td className="text-right">{fmtInt(r.batch_ms_p95)}</td><td className="text-right">{fmtInt(r.input_rows)}</td></tr>)}</tbody></table>
              ) : <EmptyState>No stream metrics yet.</EmptyState>;
            }}</Async>
            {hs && <p className="mt-2 text-[11px] text-slate-400">dead letters {fmtInt(hs.dead_letters)} · late events {fmtInt(hs.late_events)}</p>}
          </Card>
        </div>
      </div>
    </>
  );
}
