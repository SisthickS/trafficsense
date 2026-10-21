"use client";

import { Suspense, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi } from "@/lib/hooks";
import { API_BASE, figureUrl, qs } from "@/lib/api";
import { CATEGORIES, QUESTION_CHARTS, type Row } from "@/lib/charts";
import { ChartCard, TABLE_CAP } from "@/components/QuestionChart";
import { Async, Badge, Button, Card, DataTable, ErrorState, Field, Input, PageHeader, Pager, Pills, SectionTitle, Select, Skeleton, type Column } from "@/components/ui";
import type { Question, QuestionDetail, TableData } from "@/lib/types";
import { fmtNum, md } from "@/lib/format";


function TableExplorer({ qid, tables }: { qid: string; tables: string[] }) {
  const [name, setName] = useState(tables[0]);
  const [offset, setOffset] = useState(0);
  const limit = 15;
  const t = tables.includes(name) ? name : tables[0];
  const res = useApi<TableData>(`/api/v1/analytics/questions/${qid}/tables/${t}${qs({ limit, offset })}`);
  const cols: Column<Row>[] = (res.data?.columns ?? []).map((c) => ({
    key: c, header: c.replace(/_/g, " "), align: typeof res.data?.rows[0]?.[c] === "number" ? "right" : "left",
    render: (r) => (typeof r[c] === "number" ? (Number.isInteger(r[c]) ? (r[c] as number).toLocaleString("en-US") : fmtNum(r[c] as number, 3)) : String(r[c] ?? "–")),
    sort: (r) => r[c] as any,
  }));
  async function download() {
    const all = await fetch(`${API_BASE}/api/v1/analytics/questions/${qid}/tables/${t}?limit=${TABLE_CAP}`).then((r) => r.json());
    const head = all.columns as string[];
    const csv = [head.join(","), ...all.rows.map((r: Row) => head.map((c) => JSON.stringify(r[c] ?? "")).join(","))].join("\n");
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
    a.download = `${qid}_${t}.csv`;
    a.click();
  }
  return (
    <Card>
      <SectionTitle title="Result tables" hint="Rows as stored in MongoDB (analysis_results); click a header to sort the visible page."
        right={<div className="flex items-center gap-2"><Select aria-label="Table" value={t} onChange={(e) => { setName(e.target.value); setOffset(0); }}>{tables.map((x) => <option key={x}>{x}</option>)}</Select><Button variant="soft" size="sm" onClick={download}>Download CSV</Button></div>} />
      <Async res={res} height={220}>{(d) => (<><DataTable columns={cols} rows={d.rows as Row[]} rowKey={(r) => JSON.stringify(r).slice(0, 80) + offset} dense /><Pager total={d.n_rows} limit={limit} offset={offset} onChange={setOffset} /></>)}</Async>
    </Card>
  );
}

function QuestionView({ qid, highlight }: { qid: string; highlight: number | null }) {
  const q = useApi<QuestionDetail>(`/api/v1/analytics/questions/${qid}`);
  const [figs, setFigs] = useState(false);
  const specs = QUESTION_CHARTS[qid] ?? [];
  return (
    <Async res={q} height={300}>{(d) => (
      <div className="space-y-5" data-testid="question-view" data-qid={d.qid}>
        <Card>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="max-w-3xl">
              <Badge>{d.qid.toUpperCase()}</Badge>
              <h2 className="mt-2 text-xl font-semibold tracking-tight text-ink">{d.title}</h2>
              <p className="mt-1 text-sm italic text-muted">{d.question}</p>
            </div>
            <div className="flex flex-wrap gap-2">
              {Object.entries(d.metrics).filter(([, v]) => typeof v === "number" || typeof v === "string").slice(0, 5).map(([k, v]) => (
                <div key={k} className="rounded-xl bg-lav-50 px-3 py-2 ring-1 ring-inset ring-lav-200"><p className="text-[10px] uppercase tracking-wide text-muted">{k.replace(/_/g, " ")}</p><p className="text-sm font-semibold text-ink">{typeof v === "number" ? (Math.abs(v) < 1 && v !== 0 ? fmtNum(v, 3) : fmtNum(v, Number.isInteger(v) ? 0 : 2)) : String(v)}</p></div>
              ))}
            </div>
          </div>
        </Card>
        <div className="grid gap-4 md:grid-cols-2">
          {specs.map((s) => <ChartCard key={s.id} qid={qid} spec={s} highlight={highlight} />)}
        </div>
        <div className="grid gap-4 lg:grid-cols-3">
          <Card><SectionTitle title="Key findings" /><ul className="space-y-2 text-sm leading-relaxed text-ink">{d.findings.map((f, i) => <li key={i} className="flex gap-2"><span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-lav-600" /><span dangerouslySetInnerHTML={{ __html: md(f) }} /></li>)}</ul></Card>
          <Card><SectionTitle title="Recommendations" /><ul className="space-y-2 text-sm leading-relaxed text-ink">{d.implications.map((f, i) => <li key={i} className="flex gap-2"><span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-teal-500" /><span dangerouslySetInnerHTML={{ __html: md(f) }} /></li>)}</ul></Card>
          <Card><SectionTitle title="Limitations" /><ul className="space-y-2 text-sm leading-relaxed text-muted">{d.limitations.length ? d.limitations.map((f, i) => <li key={i} className="flex gap-2"><span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-amber-400" /><span dangerouslySetInnerHTML={{ __html: md(f) }} /></li>) : <li>None recorded.</li>}</ul></Card>
        </div>
        <TableExplorer key={d.qid} qid={d.qid} tables={d.tables} />
        {d.figures.length > 0 && (
          <Card>
            <SectionTitle title="Spark job figures" hint="Static charts rendered by the analytics job (served by the API)." right={<Button variant="soft" size="sm" onClick={() => setFigs((x) => !x)}>{figs ? "Hide" : "Show"} {d.figures.length} figure{d.figures.length > 1 ? "s" : ""}</Button>} />
            {figs && <div className="grid gap-4 md:grid-cols-2">{d.figures.map((f) => /* eslint-disable-next-line @next/next/no-img-element */ <img key={f} src={figureUrl(f)} alt={`${d.qid} ${f}`} className="w-full rounded-xl border border-lav-200" loading="lazy" />)}</div>}
          </Card>
        )}
      </div>
    )}</Async>
  );
}

function AnalyticsInner() {
  const router = useRouter();
  const sp = useSearchParams();
  const questions = useApi<Question[]>("/api/v1/analytics/questions");
  const [cat, setCat] = useState<string>("all");
  const [search, setSearch] = useState("");
  const [sensor, setSensor] = useState("");
  const selected = sp.get("q") && /^q([1-9]|1[0-8])$/.test(sp.get("q")!) ? sp.get("q")! : "q1";
  const catQids = (CATEGORIES.find((c) => c.id === cat) as any)?.qids as string[] | undefined;
  const list = (questions.data ?? []).filter((q) => (!catQids || catQids.includes(q.qid)) && (!search || `${q.qid} ${q.title} ${q.question}`.toLowerCase().includes(search.toLowerCase())));
  const choose = (qid: string) => router.replace(`/analytics?q=${qid}`, { scroll: false });
  const highlight = /^\d+$/.test(sensor) ? Number(sensor) : null;
  return (
    <>
      <PageHeader eyebrow="Spark analytics" title="Traffic Analytics" subtitle="The 18 analytical questions answered with Spark on 16.9 M readings. Pick a question; every chart is drawn from the result tables stored in MongoDB and served by the API." />
      <Card className="mb-5">
        <div className="grid gap-4 lg:grid-cols-[1fr_auto_auto]">
          <div>
            <p className="mb-2 text-xs font-medium text-muted">Topic</p>
            <Pills label="Topic" value={cat} onChange={setCat} options={CATEGORIES.map((c) => ({ value: c.id, label: c.label }))} />
          </div>
          <Field label="Search questions"><Input placeholder="e.g. recovery, anomaly" value={search} onChange={(e) => setSearch(e.target.value)} aria-label="Search questions" /></Field>
          <Field label="Highlight sensor id" hint="marks the sensor in sensor-level charts"><Input inputMode="numeric" placeholder="e.g. 400586" value={sensor} onChange={(e) => setSensor(e.target.value.replace(/\D/g, ""))} aria-label="Highlight sensor id" /></Field>
        </div>
        <div className="mt-4 flex flex-wrap gap-2" role="tablist" aria-label="Questions">
          {questions.error && !questions.data && <ErrorState error={questions.error} retry={() => questions.mutate()} />}
          {list.map((q) => (
            <button key={q.qid} role="tab" aria-selected={q.qid === selected} onClick={() => choose(q.qid)} title={q.question}
              className={`rounded-xl px-3 py-2 text-left text-xs ring-1 ring-inset transition ${q.qid === selected ? "bg-lav-700 text-white ring-lav-700" : "bg-white text-ink ring-lav-200 hover:bg-lav-50"}`}>
              <span className="font-semibold">{q.qid.toUpperCase()}</span> <span className={q.qid === selected ? "text-lav-100" : "text-muted"}>{q.title}</span>
            </button>
          ))}
          {questions.data && list.length === 0 && <p className="text-sm text-muted">No question matches the filters.</p>}
        </div>
      </Card>
      <QuestionView key={selected} qid={selected} highlight={highlight} />
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<Skeleton h={400} />}><AnalyticsInner /></Suspense>;
}
