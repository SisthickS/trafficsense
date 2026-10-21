"use client";

import Link from "next/link";
import { useApi } from "@/lib/hooks";
import { Async, Badge, Card, PageHeader, SectionTitle, StatCard, cx, statusTone } from "@/components/ui";
import type { Evaluation, KindStatus, LatestPredictions, Question, QuestionDetail, StatusOverview, StreamReport } from "@/lib/types";
import { fmtAgo, fmtCompact, fmtInt, fmtNum, fmtPct, md } from "@/lib/format";

interface ClassRow { class: string; sensors: number; mean_persistence: number }

function Hero() {
  const latest = useApi<LatestPredictions>("/api/v1/predictions/latest?limit=1", { refreshMs: 20000 });
  const alerts = useApi<LatestPredictions>("/api/v1/predictions/latest?alerts_only=true&limit=1", { refreshMs: 20000 });
  const best = useApi<Evaluation>("/api/v1/models/evaluations/best");
  const persistence = useApi<Evaluation>("/api/v1/models/evaluations/persistence");
  const b = best.data?.test.cost_optimal, bf = best.data?.test.f1_optimal;
  return (
    <div className="grid gap-4 lg:grid-cols-5">
      <Card className="relative overflow-hidden bg-gradient-to-br from-lav-800 via-lav-700 to-lav-600 !border-lav-700 text-white lg:col-span-3">
        <div className="absolute -right-16 -top-16 h-56 w-56 rounded-full bg-white/10 blur-2xl" aria-hidden />
        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-lav-200">Urban traffic intelligence</p>
        <h1 className="mt-2 text-3xl font-semibold leading-tight tracking-tight sm:text-4xl">Predict congestion five minutes ahead, from raw loop-detector data to live alerts.</h1>
        <p className="mt-3 max-w-2xl text-sm leading-relaxed text-lav-100">
          TrafficSense analyses 16.9 million speed readings from 325 Bay Area freeway sensors with Apache Spark, trains MLlib models,
          replays the stream through Kafka, and serves everything from MongoDB through a FastAPI backend to this dashboard.
        </p>
        <div className="mt-5 flex flex-wrap gap-2">
          <Link href="/analytics" className="rounded-xl bg-white px-4 py-2 text-sm font-semibold text-lav-800 shadow-sm hover:bg-lav-50">Explore the analysis</Link>
          <Link href="/live" className="rounded-xl bg-white/15 px-4 py-2 text-sm font-semibold text-white ring-1 ring-inset ring-white/30 hover:bg-white/25">Watch live traffic</Link>
        </div>
      </Card>
      <div className="grid gap-4 lg:col-span-2">
        <Card>
          <p className="text-xs font-medium uppercase tracking-wide text-muted">Live congestion risk</p>
          <Async res={alerts} height={72}>{(a) => (
            <div className="mt-1 flex items-end gap-3">
              <p className="text-4xl font-semibold tracking-tight text-ink" data-testid="hero-alerts">{fmtInt(a.total)}</p>
              <p className="pb-1 text-sm text-muted">sensors at or above the {fmtNum(a.threshold, 3)} alert threshold</p>
            </div>
          )}</Async>
          <p className="mt-2 text-xs text-muted">{latest.data?.slot ? <>latest slot {latest.data.event_ts?.slice(0, 16).replace("T", " ")} UTC · {fmtInt(latest.data.total)} sensors scored</> : "no predictions stored yet"}</p>
        </Card>
        <Card>
          <p className="text-xs font-medium uppercase tracking-wide text-muted">Best model · {best.data?.label ?? "…"}</p>
          <Async res={best} height={72}>{() => (
            <div className="mt-1 grid grid-cols-3 gap-3">
              <div><p className="text-2xl font-semibold text-ink" data-testid="hero-f1">{fmtNum(bf?.f1, 3)}</p><p className="text-xs text-muted">F1 (balanced)</p></div>
              <div><p className="text-2xl font-semibold text-ink">{fmtPct(b?.recall, 1)}</p><p className="text-xs text-muted">recall (deployed)</p></div>
              <div><p className="text-2xl font-semibold text-ink">{fmtPct(best.data?.cost_vs_persistence?.saving_pct, 0)}</p><p className="text-xs text-muted">cost saved vs persistence</p></div>
            </div>
          )}</Async>
          <p className="mt-2 text-xs text-muted">persistence baseline F1 {fmtNum(persistence.data?.test.f1_optimal.f1, 3)} · PR-AUC {fmtNum(bf?.pr_auc, 3)} vs {fmtNum(persistence.data?.test.f1_optimal.pr_auc, 3)}</p>
        </Card>
      </div>
    </div>
  );
}

function Summary() {
  const status = useApi<StatusOverview>("/api/v1/status", { refreshMs: 30000 });
  const classes = useApi<ClassRow[]>("/api/v1/hotspots/classes");
  const reports = useApi<StreamReport[]>("/api/v1/streaming/reports");
  const s = status.data;
  const hot = classes.data?.find((c) => c.class === "persistent hotspot");
  const lat = reports.data?.find((r) => r.end_to_end_latency?.since_slot_complete)?.end_to_end_latency?.since_slot_complete;
  const runs = s?.pipeline.reduce((a, k) => a + k.runs, 0) ?? 0, failed = s?.pipeline.reduce((a, k) => a + k.failed, 0) ?? 0;
  return (
    <section aria-label="Summary" className="grid gap-4 sm:grid-cols-2 xl:grid-cols-6">
      <StatCard label="Sensors in registry" value={fmtInt(s?.counts.sensor_metadata)} sub={`${s?.sensors_by_status.active ?? 0} active`} icon="◉" loading={!s} />
      <StatCard label="Persistent hotspots" value={fmtInt(hot?.sensors)} sub={`of ${fmtInt(s?.counts.congestion_hotspots)} sensors`} icon="▲" tone="rose" loading={!classes.data} />
      <StatCard label="Predictions stored" value={fmtCompact(s?.counts.traffic_predictions)} sub={s?.newest_prediction_event_ts ? `newest ${s.newest_prediction_event_ts.slice(0, 10)}` : "none"} icon="◇" tone="teal" loading={!s} />
      <StatCard label="Analysis tables" value={fmtInt(s?.counts.analysis_results)} sub="18 questions in MongoDB" icon="▦" tone="amber" loading={!s} />
      <StatCard label="Pipeline runs" value={fmtInt(runs)} sub={failed ? `${failed} failed` : "none failed"} icon="↻" tone={failed ? "rose" : "green"} loading={!s} />
      <StatCard label="Stream latency p50" value={lat ? `${fmtNum(lat.p50_s, 1)} s` : "–"} sub={lat ? `p95 ${fmtNum(lat.p95_s, 1)} s (horizon 300 s)` : "run a streaming demo"} icon="⏱" tone="violet" loading={!reports.data} />
    </section>
  );
}

const STAGES: { title: string; tech: string; text: string; kinds: string[]; href: string }[] = [
  { title: "Ingest & clean", tech: "PEMS-BAY · PySpark", text: "Validated raw data, causal gap filling, 41 leakage-safe features, chronological splits.", kinds: ["preprocessing"], href: "/status" },
  { title: "Spark core", tech: "RDD · DataFrame · SQL", text: "Scala RDD module, partitioning, caching and storage benchmarks.", kinds: ["spark_benchmark"], href: "/performance" },
  { title: "Analytics Q1–Q18", tech: "Spark SQL · pandas", text: "Patterns, hotspots, onset and recovery, data quality, modelling questions.", kinds: ["analytics"], href: "/analytics" },
  { title: "MLlib models", tech: "7-stage pipeline", text: "Logistic regression, decision tree, random forest; cost-aware evaluation.", kinds: ["ml_training", "ml_evaluation"], href: "/models" },
  { title: "Kafka stream", tech: "Structured Streaming", text: "Replay producer, watermarks, windows, live 5-minute-ahead predictions.", kinds: ["streaming"], href: "/live" },
  { title: "MongoDB", tech: "8 collections", text: "Validated, indexed storage for results, predictions and analyst edits.", kinds: ["data_load"], href: "/data" },
  { title: "FastAPI", tech: "REST · CRUD", text: "Typed endpoints, validation, optimistic concurrency, model inference.", kinds: [], href: "/status" },
  { title: "Dashboard", tech: "Next.js · Plotly", text: "This application: every panel reads the API.", kinds: [], href: "/" },
];

function Pipeline() {
  const status = useApi<StatusOverview>("/api/v1/status", { refreshMs: 30000 });
  const ready = useApi<{ status: string }>("/ready", { refreshMs: 15000 });
  const by = new Map<string, KindStatus>((status.data?.pipeline ?? []).map((k) => [k.kind, k]));
  return (
    <Card>
      <SectionTitle title="Pipeline workflow" hint="From raw sensor data to this page. Status comes from the pipeline_runs collection and live health checks." />
      <ol className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4" data-testid="pipeline">
        {STAGES.map((st, i) => {
          const ks = st.kinds.map((k) => by.get(k)).filter(Boolean) as KindStatus[];
          const runs = ks.reduce((a, k) => a + k.runs, 0), failed = ks.reduce((a, k) => a + k.failed, 0);
          const live = st.title === "FastAPI" ? (ready.data ? "ready" : ready.error ? "offline" : "…") : st.title === "Dashboard" ? "serving" : null;
          const tone = live ? (live === "offline" ? "rose" : "green") : failed ? "rose" : runs ? "green" : "slate";
          const label = live ?? (runs ? `${runs} run${runs > 1 ? "s" : ""}${failed ? ` · ${failed} failed` : " · ok"}` : status.data ? "no runs" : "…");
          const when = ks.length ? ks.map((k) => k.latest_finished_at).sort().pop() : null;
          return (
            <li key={st.title} className="relative">
              <Link href={st.href} className="block h-full rounded-xl border border-lav-200 bg-lav-50/60 p-4 transition hover:border-lav-400 hover:bg-white hover:shadow-soft">
                <div className="flex items-center justify-between">
                  <span className="grid h-6 w-6 place-items-center rounded-full bg-lav-700 text-xs font-semibold text-white">{i + 1}</span>
                  <Badge tone={tone as any}>{label}</Badge>
                </div>
                <h3 className="mt-3 text-sm font-semibold text-ink">{st.title}</h3>
                <p className="text-[11px] font-medium text-lav-700">{st.tech}</p>
                <p className="mt-1.5 text-xs leading-relaxed text-muted">{st.text}</p>
                {when && <p className="mt-2 text-[11px] text-slate-400">last run {fmtAgo(when)}</p>}
              </Link>
              {i < STAGES.length - 1 && <span aria-hidden className={cx("absolute -right-2.5 top-1/2 z-10 hidden -translate-y-1/2 text-lav-400", i % 4 === 3 ? "hidden" : "lg:block")}>›</span>}
            </li>
          );
        })}
      </ol>
    </Card>
  );
}

function Insights() {
  const picks = ["q1", "q5", "q10", "q16"];
  const a = useApi<QuestionDetail>("/api/v1/analytics/questions/q1");
  const b = useApi<QuestionDetail>("/api/v1/analytics/questions/q5");
  const c = useApi<QuestionDetail>("/api/v1/analytics/questions/q10");
  const d = useApi<QuestionDetail>("/api/v1/analytics/questions/q16");
  const all = [a, b, c, d];
  return (
    <div>
      <SectionTitle title="What the data says" hint="First finding of selected questions, straight from analysis_results." right={<Link href="/analytics" className="text-xs font-medium text-lav-700 hover:underline">All 18 questions →</Link>} />
      <div className="grid gap-4 md:grid-cols-2">
        {all.map((r, i) => (
          <Card key={picks[i]}>
            <Async res={r} height={90}>{(q) => (
              <>
                <Badge>{q.qid.toUpperCase()}</Badge>
                <h3 className="mt-2 text-sm font-semibold text-ink">{q.title}</h3>
                <p className="mt-1.5 text-sm leading-relaxed text-muted" dangerouslySetInnerHTML={{ __html: md(q.findings[0] ?? "") }} />
              </>
            )}</Async>
          </Card>
        ))}
      </div>
    </div>
  );
}

export default function Page() {
  return (
    <div className="space-y-6">
      <Hero />
      <Summary />
      <Pipeline />
      <Insights />
    </div>
  );
}
