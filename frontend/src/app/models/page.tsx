"use client";

import { useState } from "react";
import { useApi } from "@/lib/hooks";
import { qs, request } from "@/lib/api";
import { PlotChart } from "@/components/PlotChart";
import { Async, Badge, Button, Card, DataTable, ErrorState, Field, Input, PageHeader, Pills, SectionTitle, StatCard, useToast, type Column } from "@/components/ui";
import type { Evaluation, MetricPoint, ModelInfo, PredictResult } from "@/lib/types";
import { PALETTE, fmtInt, fmtNum, fmtPct } from "@/lib/format";

type Point = "f1_optimal" | "cost_optimal";
const POINT_LABEL: Record<Point, string> = { f1_optimal: "F1-optimal threshold", cost_optimal: "Cost-optimal threshold (deployed)" };

function ConfusionMatrix({ ev, point }: { ev: Evaluation; point: Point }) {
  const m = ev.test[point];
  const { tp, fp, fn, tn } = m.confusion;
  const z = [[tn / (tn + fp), fp / (tn + fp)], [fn / (fn + tp), tp / (fn + tp)]];
  const counts = [[tn, fp], [fn, tp]];
  return (
    <PlotChart height={250} testId={`cm-${ev.model}`} title={`${ev.label}${ev.is_best ? " ★" : ""}`}
      data={[{ type: "heatmap", x: ["no alert", "alert"], y: ["free / slowed", "congested"], z, zmin: 0, zmax: 1, colorscale: "Purples", showscale: false,
        text: z.map((row, i) => row.map((v, j) => `${fmtInt(counts[i][j])}<br>${fmtPct(v, 1)}`)), texttemplate: "%{text}", hovertemplate: "actual %{y} → predicted %{x}<br>%{text}<extra></extra>" }]}
      layout={{ yaxis: { autorange: "reversed", type: "category" }, xaxis: { type: "category" }, margin: { l: 90, t: 36, b: 36, r: 10 } }} />
  );
}

const FEATURES: { key: string; label: string; hint: string }[] = [
  { key: "speed", label: "Speed now (mph)", hint: "required" }, { key: "speed_lag_5m", label: "Speed 5 min ago", hint: "" }, { key: "speed_lag_15m", label: "Speed 15 min ago", hint: "" },
  { key: "delta_5m", label: "Change in 5 min", hint: "" }, { key: "roll_mean_15m", label: "Mean speed, 15 min", hint: "" }, { key: "congested_frac_60m", label: "Congested share, 60 min", hint: "0–1" },
  { key: "nbr_up_speed", label: "Upstream speed", hint: "" }, { key: "nbr_down_speed", label: "Downstream speed", hint: "" }, { key: "hist_congested_rate_next", label: "Historical rate for next slot", hint: "0–1" },
];

function TryModel({ info }: { info: ModelInfo | undefined }) {
  const { notify } = useToast();
  const [sensor, setSensor] = useState("400001");
  const [inst, setInst] = useState<{ sensor_id: number | null; features: Record<string, number | null> } | null>(null);
  const [res, setRes] = useState<PredictResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [ts, setTs] = useState("");

  async function load() {
    setBusy(true);
    try {
      const r = await request<any>(`/api/v1/models/example${qs({ sensor_id: sensor, ts_utc: ts ? `${ts}:00Z` : undefined })}`);
      setInst(r.data.instance);
      setRes(null);
      notify({ kind: "ok", title: `Loaded a real feature row`, detail: `sensor ${r.data.sensor_id} at ${String(r.data.ts_utc).replace("T", " ")} UTC · actual label ${r.data.actual_label}` });
    } catch (e: any) { notify({ kind: "error", title: "Could not load a feature row", detail: e.readable ?? String(e) }); }
    setBusy(false);
  }
  async function score() {
    if (!inst) return;
    setBusy(true);
    try { setRes((await request<PredictResult>("/api/v1/models/predict", { method: "POST", json: { instances: [inst] } })).data); }
    catch (e: any) { notify({ kind: "error", title: "Scoring failed", detail: e.readable ?? String(e) }); }
    setBusy(false);
  }
  const p = res?.predictions[0];
  return (
    <Card>
      <SectionTitle title="Try the saved model" hint="POST /api/v1/models/predict runs the 7-stage Spark pipeline from disk inside the API. Load a real feature row, change values, score." right={info && <Badge tone={info.loaded ? "green" : "slate"}>{info.loaded ? "model loaded" : "loads on first use (~25 s)"}</Badge>} />
      <div className="grid gap-3 sm:grid-cols-[1fr_1fr_auto]">
        <Field label="Sensor id"><Input value={sensor} onChange={(e) => setSensor(e.target.value.replace(/\D/g, ""))} aria-label="Sensor id" /></Field>
        <Field label="Time (UTC, optional)" hint="e.g. 2017-06-05T14:00 (any split)"><Input value={ts} onChange={(e) => setTs(e.target.value)} placeholder="first test row" aria-label="Time" /></Field>
        <div className="flex items-end"><Button variant="soft" onClick={load} disabled={busy || !sensor}>Load real row</Button></div>
      </div>
      {inst && (
        <>
          <div className="mt-4 grid gap-3 sm:grid-cols-3">
            {FEATURES.map((f) => (
              <Field key={f.key} label={f.label} hint={f.hint}>
                <Input type="number" step="any" value={inst.features[f.key] ?? ""} aria-label={f.label}
                  onChange={(e) => setInst({ ...inst, features: { ...inst.features, [f.key]: e.target.value === "" ? null : Number(e.target.value) } })} />
              </Field>
            ))}
          </div>
          <div className="mt-4 flex flex-wrap items-center gap-3">
            <Button onClick={score} disabled={busy || inst.features.speed == null}>Score</Button>
            <span className="text-xs text-muted">The other {Object.keys(inst.features).length - FEATURES.length} features keep the loaded values; cleared fields are median-imputed by the pipeline.</span>
          </div>
        </>
      )}
      {p && res && (
        <div className="mt-4 grid items-center gap-4 rounded-xl bg-lav-50 p-4 ring-1 ring-inset ring-lav-200 sm:grid-cols-3" data-testid="predict-result">
          <div><p className="text-xs text-muted">P(congested in 5 min)</p><p className="text-3xl font-semibold text-ink">{fmtPct(p.probability, 2)}</p></div>
          <div><p className="text-xs text-muted">Decision at threshold {fmtNum(res.threshold, 3)} ({res.threshold_source})</p><Badge tone={p.alert ? "rose" : "green"}>{p.alert ? "ALERT" : "no alert"}</Badge></div>
          <div><p className="text-xs text-muted">{res.model}</p><p className="text-sm text-ink">{fmtNum(res.inference_ms, 0)} ms{res.cold_start ? " (cold start)" : ""} · {p.imputed_features} imputed features</p></div>
        </div>
      )}
    </Card>
  );
}

export default function Page() {
  const evals = useApi<Evaluation[]>("/api/v1/models/evaluations");
  const info = useApi<ModelInfo>("/api/v1/models/info");
  const [point, setPoint] = useState<Point>("cost_optimal");
  const rows = evals.data ?? [];
  const best = rows.find((e) => e.is_best);
  const pers = rows.find((e) => e.model === "persistence");
  const cols: Column<Evaluation>[] = [
    { key: "label", header: "Model", render: (e) => <span className="font-semibold">{e.label}{e.is_best && <Badge tone="violet"> best</Badge>}{e.kind === "baseline" && <Badge tone="slate"> baseline</Badge>}</span> },
    ...([["precision", "Precision"], ["recall", "Recall"], ["f1", "F1"], ["roc_auc", "ROC-AUC"], ["pr_auc", "PR-AUC"], ["onset_recall", "Onset recall"], ["false_alarm_rate", "False-alarm rate"]] as [keyof MetricPoint, string][]).map(([k, h]): Column<Evaluation> => ({
      key: k, header: h, align: "right", sort: (e) => e.test[point][k] as number, render: (e) => fmtNum(e.test[point][k] as number, k === "false_alarm_rate" ? 4 : 3),
    })),
    { key: "threshold", header: "Threshold", align: "right", render: (e) => fmtNum(e.test[point].threshold, 3) },
    { key: "cost", header: "Cost / 1,000", align: "right", sort: (e) => e.test[point].cost_per_1000, render: (e) => fmtNum(e.test[point].cost_per_1000, 1) },
  ];
  const metrics: (keyof MetricPoint)[] = ["precision", "recall", "f1", "roc_auc", "pr_auc"];
  const withCI = rows.filter((e) => e.cost_vs_persistence && e.kind === "model");
  return (
    <>
      <PageHeader eyebrow="Spark MLlib · chronological evaluation" title="Model Evaluation"
        subtitle="Three 7-stage MLlib pipelines, tuned on the validation period and evaluated once on the untouched test period (3.3 M readings). Thresholds are chosen on validation only. Missed congestion is costed 10× a false alarm."
        right={<Pills<Point> label="Operating point" value={point} onChange={setPoint} options={(Object.keys(POINT_LABEL) as Point[]).map((v) => ({ value: v, label: POINT_LABEL[v] }))} />} />
      {evals.error && !evals.data ? <ErrorState error={evals.error} retry={() => evals.mutate()} /> : (
        <>
          <section className="mb-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4" aria-label="Best model">
            <StatCard label="Selected model" value={best?.label ?? "–"} sub="lowest validation cost" icon="★" loading={!evals.data} />
            <StatCard label={`F1 · ${point === "f1_optimal" ? "balanced" : "deployed"}`} value={fmtNum(best?.test[point].f1, 3)} sub={`persistence ${fmtNum(pers?.test[point].f1, 3)}`} icon="F" tone="teal" loading={!evals.data} />
            <StatCard label="Recall (missed congestion)" value={fmtPct(best?.test[point].recall, 1)} sub={`miss rate ${fmtPct(best?.test[point].miss_rate, 1)} vs persistence ${fmtPct(pers?.test[point].miss_rate, 1)}`} icon="◎" tone="rose" loading={!evals.data} />
            <StatCard label="Cost saved vs persistence" value={fmtPct(best?.cost_vs_persistence?.saving_pct, 0)} sub={best?.cost_vs_persistence ? `95% CI ${fmtInt(best.cost_vs_persistence.saving_ci_low)}–${fmtInt(best.cost_vs_persistence.saving_ci_high)} cost units` : ""} icon="$" tone="green" loading={!evals.data} />
          </section>
          <Card className="mb-5">
            <SectionTitle title="Test-period metrics" hint={`${POINT_LABEL[point]}; click a header to sort. Data: model_evaluations collection.`} />
            <Async res={evals} height={220}>{(d) => <DataTable columns={cols} rows={d} rowKey={(e) => e.model} />}</Async>
          </Card>
          <div className="mb-5 grid gap-5 lg:grid-cols-2">
            <Card>
              <PlotChart title="Quality metrics by model" height={320} testId="metric-bars"
                data={metrics.map((k, i) => ({ type: "bar", name: k.replace("_", "-").toUpperCase(), x: rows.map((e) => e.label), y: rows.map((e) => e.test[point][k]), marker: { color: PALETTE[i] }, hovertemplate: "%{x}<br>%{y:.3f}<extra>" + k + "</extra>" }))}
                layout={{ barmode: "group", yaxis: { range: [0.5, 1.02], title: { text: "score" } }, xaxis: { type: "category" } }} />
            </Card>
            <Card>
              <PlotChart title={`Expected cost per 1,000 readings (FN ${best?.cost_model.fn ?? 10} : FP ${best?.cost_model.fp ?? 1})`} height={320} testId="cost-bars"
                data={[{ type: "bar", x: rows.map((e) => e.label), y: rows.map((e) => e.test[point].cost_per_1000), marker: { color: rows.map((e) => (e.is_best ? "#6a5acd" : e.kind === "baseline" ? "#c9c4ee" : "#9a8eea")) }, text: rows.map((e) => fmtNum(e.test[point].cost_per_1000, 1)), textposition: "outside", hovertemplate: "%{x}: %{y:.1f}<extra></extra>" }]}
                layout={{ yaxis: { title: { text: "cost units" } }, xaxis: { type: "category" }, showlegend: false }} />
            </Card>
          </div>
          <Card className="mb-5">
            <SectionTitle title="Confusion matrices (test period)" hint="Rows are the actual next-5-minute state, shown as counts and share of the actual class. Persistence (next = now) cannot anticipate onsets." />
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-5">{rows.map((e) => <ConfusionMatrix key={e.model} ev={e} point={point} />)}</div>
          </Card>
          <div className="mb-5 grid gap-5 lg:grid-cols-2">
            <Card>
              <PlotChart title="Cost saving vs persistence, 95% day-block bootstrap CI" height={300} testId="cost-ci"
                data={[{ type: "bar", x: withCI.map((e) => e.label), y: withCI.map((e) => e.cost_vs_persistence!.saving_pct), marker: { color: PALETTE[1] },
                  error_y: { type: "data", symmetric: false, color: "#1f1b3d", thickness: 1.5, width: 6,
                    array: withCI.map((e) => { const c = e.cost_vs_persistence!, base = c.total_cost / (1 - c.saving_pct); return c.saving_ci_high / base - c.saving_pct; }),
                    arrayminus: withCI.map((e) => { const c = e.cost_vs_persistence!, base = c.total_cost / (1 - c.saving_pct); return c.saving_pct - c.saving_ci_low / base; }) },
                  text: withCI.map((e) => fmtPct(e.cost_vs_persistence!.saving_pct, 1)), textposition: "inside", hovertemplate: "%{x}: %{y:.1%}<extra></extra>" }]}
                layout={{ yaxis: { tickformat: ".0%", title: { text: "cost saved" }, range: [0, 0.8] }, xaxis: { type: "category" }, showlegend: false }} />
              <p className="mt-1 text-xs text-muted">Share of bootstrap resamples of test days in which the model is cheaper than persistence: {withCI.map((e) => `${e.label} ${fmtPct(e.cost_vs_persistence!.share_boot_cheaper, 0)}`).join(" · ")}.</p>
            </Card>
            <Card>
              <PlotChart title="Recall vs false-alarm rate (both operating points)" height={300} testId="roc-points"
                data={(["f1_optimal", "cost_optimal"] as Point[]).map((pt, i) => ({ type: "scatter", mode: "markers+text", name: POINT_LABEL[pt].split(" (")[0], x: rows.map((e) => e.test[pt].false_alarm_rate), y: rows.map((e) => e.test[pt].recall), text: rows.map((e) => e.label.split(" ")[0]), textposition: "top center", marker: { size: 11, color: PALETTE[i * 2] } }))}
                layout={{ xaxis: { title: { text: "false-alarm rate on stable free-flow rows" }, tickformat: ".1%" }, yaxis: { title: { text: "recall" }, tickformat: ".0%" } }} />
            </Card>
          </div>
        </>
      )}
      <TryModel info={info.data} />
    </>
  );
}
