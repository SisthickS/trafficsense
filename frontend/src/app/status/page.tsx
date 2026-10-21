"use client";

import { useState } from "react";
import { useApi } from "@/lib/hooks";
import { qs } from "@/lib/api";
import { PlotChart } from "@/components/PlotChart";
import { Async, Badge, Card, DataTable, Field, Modal, PageHeader, Pager, SectionTitle, Select, StatCard, statusTone, type Column } from "@/components/ui";
import type { DatabaseInfo, KindStatus, ModelInfo, Paged, PipelineRun, StatusOverview } from "@/lib/types";
import { PALETTE, fmtAgo, fmtDate, fmtInt, fmtNum, fmtSeconds, titleCase } from "@/lib/format";

interface Health { status: string; version: string; env: string; uptime_s: number }
interface StreamHealth { healthy: boolean; reason?: string; seconds_since_last_commit?: number; last_batch_id?: number }
const PAGE = 12;

function System() {
  const health = useApi<Health>("/health", { refreshMs: 15000 });
  const ready = useApi<{ status: string; database: string }>("/ready", { refreshMs: 15000 });
  const status = useApi<StatusOverview>("/api/v1/status", { refreshMs: 30000 });
  const db = useApi<DatabaseInfo>("/api/v1/status/database", { refreshMs: 30000 });
  const stream = useApi<StreamHealth>("/api/v1/streaming/health?max_age_s=120", { refreshMs: 30000 });
  const model = useApi<ModelInfo>("/api/v1/models/info");
  const up = health.data?.uptime_s;
  return (
    <>
      <section className="mb-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-5" aria-label="System status">
        <StatCard label="FastAPI" value={ready.data ? "ready" : ready.error ? "offline" : "…"} sub={health.data ? `v${health.data.version} · ${health.data.env} · up ${up! > 3600 ? `${(up! / 3600).toFixed(1)} h` : `${Math.round(up! / 60)} min`}` : ""} icon="⚙" tone={ready.data ? "green" : "rose"} />
        <StatCard label="MongoDB" value={db.data ? `v${db.data.server_version}` : db.error ? "offline" : "…"} sub={db.data ? `ping ${fmtNum(db.data.ping_ms, 0)} ms · ${fmtNum(db.data.data_mb, 1)} MB data` : ""} icon="◆" tone={db.data ? "green" : "rose"} />
        <StatCard label="Streaming job" value={stream.data ? (stream.data.healthy ? "running" : "idle") : "…"} sub={stream.data?.healthy ? `batch ${stream.data.last_batch_id}` : stream.data?.seconds_since_last_commit != null ? `last commit ${fmtAgo(new Date(Date.now() - stream.data.seconds_since_last_commit * 1000).toISOString())}` : stream.data?.reason} icon="↻" tone={stream.data?.healthy ? "green" : "slate"} />
        <StatCard label="Saved model" value={model.data?.available ? (model.data.loaded ? "loaded" : "on disk") : model.data ? "missing" : "…"} sub={model.data?.label ? `${model.data.label} · ${model.data.stages?.length ?? 0} stages` : ""} icon="★" tone="violet" />
        <StatCard label="Write protection" value={status.data?.write_protection === "api-key" ? "API key" : status.data ? "open (dev)" : "…"} sub="set TRAFFICSENSE_API_KEY to require a key" icon="⚿" tone={status.data?.write_protection === "api-key" ? "green" : "amber"} />
      </section>
      <div className="mb-5 grid gap-5 lg:grid-cols-5">
        <Card className="lg:col-span-3">
          <SectionTitle title="MongoDB collections" hint={db.data ? `${db.data.database} · storage ${fmtNum(db.data.storage_mb, 1)} MB · indexes ${fmtNum(db.data.index_mb, 1)} MB` : undefined} />
          <Async res={db} height={300}>{(d) => (
            <DataTable dense rowKey={(c) => c.name} rows={d.collections} columns={[
              { key: "name", header: "Collection", sort: (c) => c.name, render: (c) => <span className="font-mono text-xs font-semibold text-lav-800">{c.name}</span> },
              { key: "docs", header: "Documents", align: "right", sort: (c) => c.documents, render: (c) => fmtInt(c.documents) },
              { key: "ix", header: "Indexes", render: (c) => <span title={c.indexes.join(", ")}>{c.indexes.length} <span className="text-[11px] text-slate-400">{c.indexes.filter((i) => i !== "_id_").slice(0, 2).join(", ")}{c.indexes.length > 3 ? "…" : ""}</span></span> },
              { key: "v", header: "Schema validator", render: (c) => (c.validator ? <Badge tone="green">$jsonSchema</Badge> : <Badge tone="slate">none</Badge>) }]} maxHeight={420} />
          )}</Async>
        </Card>
        <Card className="lg:col-span-2">
          <Async res={db} height={300}>{(d) => (
            <PlotChart title="Documents per collection" height={380} testId="collections-chart"
              data={[{ type: "bar", orientation: "h", x: [...d.collections].filter((c) => c.documents > 0).sort((a, b) => a.documents - b.documents).map((c) => c.documents), y: [...d.collections].filter((c) => c.documents > 0).sort((a, b) => a.documents - b.documents).map((c) => c.name), marker: { color: PALETTE[0] }, hovertemplate: "%{y}: %{x:,}<extra></extra>" }]}
              layout={{ xaxis: { type: "log", title: { text: "documents (log)" } }, yaxis: { type: "category" }, margin: { l: 130 }, showlegend: false }} />
          )}</Async>
        </Card>
      </div>
    </>
  );
}

function Pipeline() {
  const status = useApi<StatusOverview>("/api/v1/status", { refreshMs: 30000 });
  return (
    <Card className="mb-5">
      <SectionTitle title="Pipeline status by stage" hint="Latest run per kind, from pipeline_runs." />
      <Async res={status} height={110}>{(s) => (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-7">
          {s.pipeline.map((k: KindStatus) => (
            <div key={k.kind} className="rounded-xl border border-lav-200 bg-lav-50/60 p-3">
              <div className="flex items-center justify-between"><p className="text-xs font-semibold text-ink">{titleCase(k.kind)}</p><Badge tone={k.failed ? "rose" : "green"}>{k.failed ? `${k.failed} failed` : "ok"}</Badge></div>
              <p className="mt-2 text-2xl font-semibold text-ink">{k.runs}</p><p className="text-[11px] text-muted">runs · last {fmtAgo(k.latest_finished_at)}</p>
            </div>
          ))}
        </div>
      )}</Async>
    </Card>
  );
}

function History() {
  const [kind, setKind] = useState("");
  const [st, setSt] = useState("");
  const [offset, setOffset] = useState(0);
  const [open, setOpen] = useState<PipelineRun | null>(null);
  const status = useApi<StatusOverview>("/api/v1/status");
  const res = useApi<Paged<PipelineRun>>(`/api/v1/pipeline/runs${qs({ limit: PAGE, offset, kind, status: st })}`);
  const analytics = useApi<Paged<PipelineRun>>("/api/v1/pipeline/runs?kind=analytics&limit=50");
  const cols: Column<PipelineRun>[] = [
    { key: "kind", header: "Kind", render: (r) => <Badge>{titleCase(r.kind)}</Badge> },
    { key: "name", header: "Run", render: (r) => <span className="font-semibold">{r.name}</span> },
    { key: "status", header: "Status", render: (r) => <Badge tone={statusTone(r.status)}>{r.status}</Badge> },
    { key: "dur", header: "Duration", align: "right", sort: (r) => r.duration_seconds, render: (r) => fmtSeconds(r.duration_seconds) },
    { key: "fin", header: "Finished", sort: (r) => r.finished_at, render: (r) => <span title={r.finished_at}>{fmtDate(r.finished_at)}</span> },
    { key: "sum", header: "Summary", render: (r) => <span className="text-xs text-muted">{Object.entries(r.summary ?? {}).filter(([, v]) => typeof v !== "object").slice(0, 3).map(([k, v]) => `${k.replace(/_/g, " ")}: ${typeof v === "number" ? fmtNum(v, Number.isInteger(v) ? 0 : 2) : v}`).join(" · ") || "–"}</span> },
  ];
  const aj = [...(analytics.data?.items ?? [])].sort((a, b) => Number(a.name.slice(1)) - Number(b.name.slice(1)));
  return (
    <>
      <Card className="mb-5">
        <PlotChart title="Analysis history: Spark job duration per question" height={280} testId="history-chart"
          data={[{ type: "bar", x: aj.map((r) => r.name.toUpperCase()), y: aj.map((r) => r.duration_seconds), marker: { color: aj.map((r) => (r.status === "succeeded" ? PALETTE[0] : "#d9534f")) }, hovertemplate: "%{x}: %{y:.1f} s<extra></extra>" }]}
          layout={{ xaxis: { type: "category" }, yaxis: { title: { text: "seconds" }, type: "log" }, showlegend: false }} />
      </Card>
      <Card>
        <SectionTitle title="Run history" hint="Every pipeline run recorded in MongoDB. Click a row for its full record."
          right={<div className="flex gap-3"><Field label="Kind"><Select value={kind} onChange={(e) => { setKind(e.target.value); setOffset(0); }} aria-label="Kind filter"><option value="">All</option>{(status.data?.pipeline ?? []).map((k) => <option key={k.kind} value={k.kind}>{titleCase(k.kind)}</option>)}</Select></Field>
            <Field label="Status"><Select value={st} onChange={(e) => { setSt(e.target.value); setOffset(0); }} aria-label="Status filter"><option value="">All</option><option value="succeeded">succeeded</option><option value="failed">failed</option></Select></Field></div>} />
        <Async res={res} height={360}>{(d) => (<><DataTable columns={cols} rows={d.items} rowKey={(r) => `${r.kind}|${r.name}|${r.finished_at}`} dense onRowClick={setOpen} /><Pager total={d.total} limit={d.limit} offset={d.offset} onChange={setOffset} /></>)}</Async>
      </Card>
      <Modal open={open !== null} onClose={() => setOpen(null)} title={open ? `${titleCase(open.kind)} · ${open.name}` : ""} wide>
        <pre className="scroll-thin max-h-[60vh] overflow-auto rounded-xl bg-lav-50 p-4 text-xs leading-relaxed text-ink" data-testid="run-json">{open ? JSON.stringify(open, (k, v) => (k === "candidates_table" || (Array.isArray(v) && v.length > 12) ? `[${(v as unknown[]).length} items]` : v), 2) : ""}</pre>
        {open?.artifacts?.length ? <p className="mt-3 text-xs text-muted">Artifacts: {open.artifacts.join(" · ")}</p> : null}
      </Modal>
    </>
  );
}

export default function Page() {
  return (
    <>
      <PageHeader eyebrow="Operations" title="Analysis History & System Status" subtitle="Live health of every component the dashboard depends on, the MongoDB schema (documents, indexes, validators), and the history of all pipeline runs." />
      <System />
      <Pipeline />
      <History />
    </>
  );
}
