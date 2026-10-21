/**
 * Declarative Plotly charts for the 18 analytical questions. Every chart names an API result table (served from MongoDB
 * `analysis_results`) and which columns to plot; `buildFigure` turns the fetched rows into Plotly traces. No numbers live here.
 */
import { PALETTE } from "@/lib/format";

export type Row = Record<string, any>;

export interface ChartSpec {
  id: string;
  title: string;
  table: string;
  kind: "bar" | "hbar" | "line" | "scatter" | "heatmap" | "matrix" | "hist" | "pie";
  x?: string;
  y?: string | string[];
  z?: string;
  color?: string;               // numeric -> colour scale, text -> one trace per value
  text?: string;                // hover label column
  split?: string;               // one trace per distinct value of this column
  size?: string;
  matrixCols?: string[];        // kind "matrix": wide table (row label = x, value columns listed here)
  top?: number;                 // keep the first N rows (after sorting)
  sortBy?: string;
  desc?: boolean;
  xTitle?: string;
  yTitle?: string;
  pct?: boolean;                // format the value axis as percent
  stack?: boolean;
  diag?: boolean;               // draw y = x
  xIndex?: boolean;             // x = row number
  height?: number;
  note?: string;
  labels?: Record<string, string>;
}

const DOW = ["", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

export const CATEGORIES = [
  { id: "all", label: "All questions" },
  { id: "temporal", label: "Time patterns", qids: ["q1", "q2", "q3", "q4"] },
  { id: "spatial", label: "Hotspots & network", qids: ["q5", "q6", "q7", "q8", "q9", "q12"] },
  { id: "dynamics", label: "Onset & recovery", qids: ["q10", "q11"] },
  { id: "quality", label: "Data quality", qids: ["q13", "q14"] },
  { id: "modeling", label: "Prediction & streaming", qids: ["q15", "q16", "q17", "q18"] },
] as const;

export const QUESTION_CHARTS: Record<string, ChartSpec[]> = {
  q1: [
    { id: "hourly", title: "Share of readings congested, by hour", table: "hourly_profile", kind: "line", x: "hour", y: ["workday_rate", "weekend_holiday_rate"], pct: true, xTitle: "Local hour", labels: { workday_rate: "Working days", weekend_holiday_rate: "Weekends & holidays" } },
    { id: "heat", title: "Congestion rate: weekday × hour", table: "dow_hour", kind: "heatmap", x: "hour", y: "dow", z: "rate", xTitle: "Local hour", pct: true },
    { id: "dow", title: "Mean congestion rate by weekday", table: "day_of_week", kind: "bar", x: "dow_name", y: "mean_rate", pct: true },
    { id: "daily", title: "Daily congestion rate", table: "daily", kind: "line", x: "date_local", y: "rate", pct: true, note: "Dips are weekends and holidays." },
  ],
  q2: [
    { id: "scatter", title: "Recurrence vs congestion exposure per sensor", table: "all_sensors", kind: "scatter", x: "workday_congestion_rate", y: "recurrent_share", color: "median_profile_corr", text: "sensor_id", xTitle: "Working-day congestion rate", yTitle: "Recurrent share of congestion", pct: true },
    { id: "hist", title: "Profile correlation across sensors", table: "all_sensors", kind: "hist", x: "median_profile_corr", xTitle: "Median correlation with the sensor's recurring profile" },
  ],
  q3: [
    { id: "dur", title: "Episodes by duration bucket", table: "by_duration", kind: "bar", x: "bucket", y: "episodes" },
    { id: "rec", title: "Median recovery time by duration bucket", table: "by_duration", kind: "bar", x: "bucket", y: "median_recovery_min", yTitle: "minutes" },
    { id: "period", title: "Episode length by start period", table: "by_start_period", kind: "bar", x: "period", y: ["median_duration_min", "p90_duration_min"], yTitle: "minutes", labels: { median_duration_min: "Median", p90_duration_min: "90th percentile" } },
  ],
  q4: [
    { id: "hourly", title: "Speed volatility by hour", table: "hourly", kind: "line", x: "hour", y: "vol_std_delta", split: "is_workday", yTitle: "std of 5-min speed change (mph)", labels: { true: "Working days", false: "Other days" } },
    { id: "sensors", title: "Volatility vs congestion per sensor", table: "sensors", kind: "scatter", x: "congestion_rate", y: "vol_std_delta", text: "sensor_id", pct: false, xTitle: "Congestion rate" },
    { id: "top", title: "Most volatile sensors (PM peak)", table: "top_volatile_sensors", kind: "bar", x: "sensor_id", y: "pm_peak_vol" },
  ],
  q5: [
    { id: "map", title: "Hotspot map (colour = persistence)", table: "all_sensors", kind: "scatter", x: "longitude", y: "latitude", color: "persistence", size: "mean_daily_congested_min", text: "sensor_id", xTitle: "Longitude", yTitle: "Latitude", height: 380 },
    { id: "classes", title: "Hotspot classes", table: "classes", kind: "pie", x: "class", y: "sensors" },
    { id: "stab", title: "Month-to-month rank stability", table: "monthly_stability", kind: "bar", x: "from", y: "spearman_rank_corr", yTitle: "Spearman ρ" },
  ],
  q6: [
    { id: "eta", title: "Variance explained by road characteristics (η²)", table: "effects", kind: "bar", x: "characteristic", y: ["eta_squared_pm_peak", "eta_squared_overall"], labels: { eta_squared_pm_peak: "PM peak", eta_squared_overall: "Overall" } },
    { id: "groups", title: "Mean PM-peak congestion by group", table: "groups", kind: "bar", x: "group", y: "mean_pm_peak_rate", split: "characteristic", pct: true },
    { id: "sil", title: "Sensor clustering: silhouette by k", table: "kmeans_silhouette", kind: "line", x: "k", y: "silhouette" },
  ],
  q7: [
    { id: "corr", title: "Neighbour correlation by time lag", table: "lag_curves", kind: "line", x: "lag_min", y: ["mean_corr_all_edges", "median_corr_informative"], xTitle: "Lag (min)", labels: { mean_corr_all_edges: "All edges", median_corr_informative: "Informative edges" } },
    { id: "prop", title: "P(neighbour congested) after an onset", table: "lag_curves", kind: "line", x: "lag_min", y: ["p_upstream_cong_after_downstream_onset", "p_downstream_cong_after_upstream_onset"], xTitle: "Lag (min)", pct: true, labels: { p_upstream_cong_after_downstream_onset: "Upstream after downstream onset", p_downstream_cong_after_upstream_onset: "Downstream after upstream onset" } },
    { id: "edges", title: "Edge correlation vs road distance", table: "edges", kind: "scatter", x: "distance_km", y: "corr_0", color: "peak_lag_min", xTitle: "Distance (km)", yTitle: "Same-time correlation" },
  ],
  q8: [
    { id: "sc", title: "Peak-hour residual vs congestion gap to peers", table: "all_sensors", kind: "scatter", x: "pm_residual_mph", y: "congestion_vs_peers", color: "flag", text: "sensor_id", xTitle: "PM residual vs peers (mph)", yTitle: "Congestion gap to peers" },
    { id: "flags", title: "Sensor flags", table: "flags", kind: "pie", x: "flag", y: "sensors" },
    { id: "bn", title: "Local bottlenecks (PM residual)", table: "bottlenecks", kind: "bar", x: "sensor_id", y: "pm_residual_mph" },
  ],
  q9: [
    { id: "top", title: "Highest congestion severity (CSBI)", table: "top_sensors", kind: "bar", x: "sensor_id", y: "csbi" },
    { id: "daily", title: "Network travel-time index by day", table: "daily", kind: "line", x: "date_local", y: ["network_tti", "network_peak_tti"], labels: { network_tti: "All day", network_peak_tti: "Peak" } },
    { id: "hist", title: "CSBI across sensors", table: "all_sensors", kind: "hist", x: "csbi", xTitle: "Congestion severity & burden index" },
    { id: "cor", title: "Correlation of severity components", table: "component_correlation", kind: "matrix", x: "component", matrixCols: ["frequency", "intensity", "duration", "unreliability"] },
  ],
  q10: [
    { id: "tm", title: "Next-state probabilities (Markov chain)", table: "transition_matrix", kind: "matrix", x: "from", matrixCols: ["to_free", "to_slowed", "to_congested"] },
    { id: "hour", title: "Onset probability by hour", table: "onset_hazard_by_hour", kind: "line", x: "hour", y: ["free", "slowed"], pct: true, labels: { free: "From free flow", slowed: "From slowed" } },
    { id: "speed", title: "Onset probability by current speed", table: "onset_hazard_by_speed", kind: "bar", x: "speed_band", y: "onset_prob", split: "is_workday", pct: true, labels: { true: "Working days", false: "Other days" } },
    { id: "prec", title: "Precursors of onset (standardized difference)", table: "precursors", kind: "hbar", x: "smd", y: "feature", top: 10, sortBy: "smd", desc: true },
  ],
  q11: [
    { id: "hr", title: "Median recovery time by episode end hour", table: "by_end_hour", kind: "bar", x: "end_hour", y: "median_recovery_min", yTitle: "minutes" },
    { id: "hist", title: "Recovery time across sensors", table: "all_sensors", kind: "hist", x: "median_recovery_min", xTitle: "Median recovery (min)" },
    { id: "slow", title: "Slowest-recovering sensors vs peers", table: "slow_recovery_sensors", kind: "bar", x: "sensor_id", y: "recovery_gap_vs_peers_min", top: 15, yTitle: "gap to peers (min)" },
  ],
  q12: [
    { id: "pareto", title: "Pareto curve: cumulative share of congestion minutes", table: "sensors", kind: "line", xIndex: true, y: "cum_share", xTitle: "Sensors, ranked by burden", pct: true },
    { id: "gini", title: "Concentration (Gini) by measure", table: "concentration", kind: "bar", x: "measure", y: "gini", split: "unit" },
    { id: "cells", title: "Most burdensome sensor × weekday × hour cells", table: "top_cells", kind: "bar", x: "sensor_id", y: "congested_hours" },
  ],
  q13: [
    { id: "q", title: "Reading quality", table: "quality", kind: "pie", x: "quality", y: "readings" },
    { id: "mnar", title: "Missingness is not random: congestion rate by hour", table: "mnar_by_hour", kind: "line", x: "hour", y: ["rate_complete_cells", "rate_cells_with_invalid"], pct: true, labels: { rate_complete_cells: "Complete cells", rate_cells_with_invalid: "Cells with invalid readings" } },
    { id: "worst", title: "Sensors with most unobserved readings", table: "worst_sensors", kind: "bar", x: "sensor_id", y: "not_observed_share", pct: true },
    { id: "iso", title: "Isolated sensors (no near neighbour)", table: "isolated_sensors", kind: "scatter", x: "longitude", y: "latitude", color: "nn_km", text: "sensor_id", xTitle: "Longitude", yTitle: "Latitude" },
  ],
  q14: [
    { id: "hour", title: "Anomaly kinds by hour", table: "by_hour", kind: "bar", x: "hour", y: ["traffic anomaly (corroborated)", "isolated spike", "isolated persistent", "unverifiable (no neighbours)"], stack: true },
    { id: "kinds", title: "Anomalies: slowdowns vs speed-ups", table: "kinds", kind: "bar", x: "kind", y: ["slowdown", "speed-up"] },
    { id: "sus", title: "Most suspect sensors", table: "suspect_sensors", kind: "bar", x: "sensor_id", y: "anomalies" },
  ],
  q15: [
    { id: "tp", title: "Stream throughput by micro-batch size", table: "throughput", kind: "bar", x: "max_files_per_trigger", y: "rows_per_s", split: "shuffle_partitions", yTitle: "rows / s", xTitle: "max files per trigger", labels: {} },
    { id: "lat", title: "Window-result latency (ms)", table: "latency", kind: "bar", x: "trigger", y: ["latency_ms_p50", "latency_ms_p95"], labels: { latency_ms_p50: "p50", latency_ms_p95: "p95" } },
    { id: "score", title: "Model scoring throughput", table: "scoring", kind: "bar", x: "batch", y: "rows_per_s", yTitle: "rows / s" },
  ],
  q16: [
    { id: "pr", title: "Test precision / recall / F1", table: "test_metrics", kind: "bar", x: "model", y: ["precision", "recall", "f1"] },
    { id: "auc", title: "Threshold-free quality (PR-AUC, ROC-AUC)", table: "test_metrics", kind: "bar", x: "model", y: ["pr_auc", "roc_auc"] },
    { id: "imp", title: "Feature importance (best tree model)", table: "importances", kind: "hbar", x: "importance", y: "feature", top: 12, sortBy: "importance", desc: true },
  ],
  q17: [
    { id: "abl", title: "Contribution of each feature group (PR-AUC lost when removed)", table: "ablation", kind: "bar", x: "group", y: "contribution_pr_auc" },
    { id: "var", title: "Model variants (PR-AUC)", table: "variants", kind: "bar", x: "variant", y: "pr_auc" },
    { id: "imp", title: "Importance share by group and model", table: "importance_by_group", kind: "bar", x: "group", y: ["Decision tree", "Gradient-boosted trees", "Random forest"] },
  ],
  q18: [
    { id: "weeks", title: "F1 by test week vs persistence", table: "weeks", kind: "line", x: "week", y: ["f1", "persistence_f1"], labels: { f1: "Model", persistence_f1: "Persistence" } },
    { id: "hours", title: "F1 by hour of day vs persistence", table: "hours", kind: "line", x: "hour", y: ["f1", "persistence_f1"], labels: { f1: "Model", persistence_f1: "Persistence" } },
    { id: "cal", title: "Calibration (predicted vs observed)", table: "calibration", kind: "scatter", x: "mean_predicted", y: "observed_rate", diag: true, xTitle: "Mean predicted probability", yTitle: "Observed rate" },
    { id: "hold", title: "Unseen-sensor hold-out", table: "sensor_holdout", kind: "bar", x: "setting", y: ["f1", "pr_auc", "onset_recall"] },
  ],
};

/* ------------------------------------------------------------------------------------------------------------ builders */
const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const label = (spec: ChartSpec, key: string) => spec.labels?.[key] ?? key.replace(/_/g, " ");
const uniq = (rows: Row[], col: string) => [...new Set(rows.map((r) => String(r[col])))];

export interface Figure { data: Record<string, any>[]; layout: Record<string, any> }

export function buildFigure(spec: ChartSpec, input: Row[], opts: { highlightSensor?: number | null } = {}): Figure {
  let rows = input;
  if (spec.sortBy) {
    const k = spec.sortBy;
    rows = [...rows].sort((a, b) => ((num(a[k]) ?? -Infinity) - (num(b[k]) ?? -Infinity)) * (spec.desc ? -1 : 1));
  }
  if (spec.top) rows = rows.slice(0, spec.top);
  const hl = opts.highlightSensor ?? null;
  const layout: Record<string, any> = { xaxis: { title: { text: spec.xTitle ?? "" } }, yaxis: { title: { text: spec.yTitle ?? "" } } };
  if (spec.pct) layout.yaxis.tickformat = ".0%";
  const ys = Array.isArray(spec.y) ? spec.y : spec.y ? [spec.y] : [];
  const data: Record<string, any>[] = [];
  const xs = spec.xIndex ? rows.map((_, i) => i + 1) : spec.x ? rows.map((r) => r[spec.x as string]) : [];
  const barColors = (rs: Row[], base: string) => rs.map((r) => (hl != null && r.sensor_id === hl ? "#e8836a" : base));

  switch (spec.kind) {
    case "bar":
    case "line": {
      const mode = spec.kind === "line" ? "lines+markers" : undefined;
      const mk = (name: string, x: any[], y: any[], i: number, rs: Row[]) => ({
        type: spec.kind === "line" ? "scatter" : "bar", mode, name, x, y, marker: spec.kind === "bar" ? { color: barColors(rs, PALETTE[i % PALETTE.length]) } : { size: 4 },
        line: { width: 2 }, hovertemplate: `${name}: %{y:${spec.pct ? ".1%" : ",.4~g"}}<br>%{x}<extra></extra>`,
      });
      if (spec.split) {
        uniq(rows, spec.split).forEach((v, i) => {
          const rs = rows.filter((r) => String(r[spec.split as string]) === v);
          data.push(mk(spec.labels?.[v] ?? v, spec.xIndex ? rs.map((_, j) => j + 1) : rs.map((r) => r[spec.x as string]), rs.map((r) => r[ys[0]]), i, rs));
        });
      } else {
        ys.forEach((y, i) => data.push(mk(label(spec, y), xs, rows.map((r) => r[y]), i, rows)));
      }
      if (spec.kind === "bar") { layout.xaxis.type = "category"; layout.barmode = spec.stack ? "stack" : "group"; }
      if (ys.length < 2 && !spec.split) layout.showlegend = false;
      break;
    }
    case "hbar": {
      const y = ys.length ? ys[0] : (spec.y as string);
      const sorted = [...rows].reverse();
      data.push({ type: "bar", orientation: "h", x: sorted.map((r) => r[spec.x as string]), y: sorted.map((r) => r[y]), marker: { color: PALETTE[0] }, hovertemplate: "%{y}: %{x:,.4~g}<extra></extra>" });
      layout.yaxis = { ...layout.yaxis, type: "category" };
      layout.margin = { l: 150 };
      layout.showlegend = false;
      break;
    }
    case "scatter": {
      const y = ys[0];
      const hoverText = (rs: Row[]) => rs.map((r) => (spec.text ? `${spec.text.replace("_", " ")} ${r[spec.text]}` : ""));
      const size = (rs: Row[]) => rs.map((r) => (hl != null && r.sensor_id === hl ? 16 : spec.size ? 5 + 11 * Math.sqrt(Math.max(0, num(r[spec.size]) ?? 0) / Math.max(1, ...rows.map((q) => num(q[spec.size as string]) ?? 0))) : 7));
      const colorIsText = !!spec.color && rows.some((r) => typeof r[spec.color as string] === "string");
      if (spec.color && colorIsText) {
        uniq(rows, spec.color).forEach((v, i) => {
          const rs = rows.filter((r) => String(r[spec.color as string]) === v);
          data.push({ type: "scatter", mode: "markers", name: v, x: rs.map((r) => r[spec.x as string]), y: rs.map((r) => r[y]), text: hoverText(rs), marker: { size: size(rs), color: PALETTE[i % PALETTE.length], opacity: 0.8, line: { width: 0.5, color: "#fff" } }, hovertemplate: "%{text}<br>x %{x:,.4~g}, y %{y:,.4~g}<extra>" + v + "</extra>" });
        });
      } else {
        data.push({
          type: "scatter", mode: "markers", name: spec.title, x: rows.map((r) => r[spec.x as string]), y: rows.map((r) => r[y]), text: hoverText(rows),
          marker: { size: size(rows), opacity: 0.85, line: { width: 0.5, color: "#fff" }, ...(spec.color ? { color: rows.map((r) => r[spec.color as string]), colorscale: "Viridis", showscale: true, colorbar: { title: { text: label(spec, spec.color), side: "right" }, thickness: 12 } } : { color: PALETTE[0] }) },
          hovertemplate: "%{text}<br>x %{x:,.4~g}, y %{y:,.4~g}<extra></extra>",
        });
      }
      if (spec.diag) {
        const v = rows.flatMap((r) => [num(r[spec.x as string]), num(r[y])]).filter((n): n is number => n != null);
        data.push({ type: "scatter", mode: "lines", x: [Math.min(...v), Math.max(...v)], y: [Math.min(...v), Math.max(...v)], line: { dash: "dot", color: "#9a8eea" }, name: "ideal", hoverinfo: "skip" });
      }
      if (spec.x === "longitude") layout.yaxis.scaleanchor = "x";
      layout.showlegend = !!spec.color && colorIsText || !!spec.diag;
      if (spec.pct) layout.xaxis.tickformat = ".0%";
      break;
    }
    case "heatmap": {
      const xv = [...new Set(rows.map((r) => r[spec.x as string]))].sort((a, b) => a - b);
      const yv = [...new Set(rows.map((r) => r[spec.y as string]))].sort((a, b) => a - b);
      const z = yv.map((yy) => xv.map((xx) => rows.find((r) => r[spec.x as string] === xx && r[spec.y as string] === yy)?.[spec.z as string] ?? null));
      data.push({ type: "heatmap", x: xv, y: yv.map((v) => (spec.y === "dow" ? DOW[v] ?? v : v)), z, colorscale: "Purples", reversescale: false, colorbar: { thickness: 12, tickformat: spec.pct ? ".0%" : undefined }, hovertemplate: "%{y} %{x}: %{z:.3~g}<extra></extra>" });
      layout.yaxis = { ...layout.yaxis, autorange: "reversed", type: "category" };
      layout.xaxis = { ...layout.xaxis, dtick: 2 };
      layout.yaxis.tickformat = undefined;
      break;
    }
    case "matrix": {
      const cols = spec.matrixCols ?? [];
      const z = rows.map((r) => cols.map((c) => num(r[c])));
      data.push({ type: "heatmap", x: cols.map((c) => c.replace(/_/g, " ")), y: rows.map((r) => String(r[spec.x as string])), z, colorscale: "Purples", text: z.map((row) => row.map((v) => (v == null ? "" : v.toFixed(3)))), texttemplate: "%{text}", hovertemplate: "%{y} → %{x}: %{z:.4f}<extra></extra>", colorbar: { thickness: 12 } });
      layout.yaxis = { ...layout.yaxis, autorange: "reversed", type: "category" };
      break;
    }
    case "hist": {
      data.push({ type: "histogram", x: rows.map((r) => r[spec.x as string]).filter((v) => num(v) != null), marker: { color: PALETTE[0], line: { color: "#fff", width: 0.5 } }, nbinsx: 30, hovertemplate: "%{x}: %{y} sensors<extra></extra>" });
      layout.yaxis.title = { text: "sensors" };
      layout.showlegend = false;
      break;
    }
    case "pie": {
      data.push({ type: "pie", labels: rows.map((r) => r[spec.x as string]), values: rows.map((r) => r[ys[0]]), hole: 0.5, marker: { colors: PALETTE }, textinfo: "label+percent", hovertemplate: "%{label}: %{value:,}<extra></extra>", sort: false });
      layout.showlegend = false;
      layout.margin = { l: 16, r: 16, t: 16, b: 16 };
      break;
    }
  }
  return { data, layout };
}
