"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useApi } from "@/lib/hooks";
import { qs } from "@/lib/api";
import { PlotChart } from "@/components/PlotChart";
import { Async, Badge, Card, DataTable, EmptyState, Field, Input, PageHeader, Pager, Pills, SectionTitle, Select, StatCard, statusTone, type Column } from "@/components/ui";
import type { Hotspot, Paged, Prediction, Sensor } from "@/lib/types";
import { PALETTE, fmtDate, fmtInt, fmtNum, fmtPct } from "@/lib/format";

interface ClassRow { class: string; sensors: number; mean_persistence: number; mean_daily_congested_min: number }
const PAGE = 12;

function SensorPanel({ id }: { id: number }) {
  const sensor = useApi<Sensor>(`/api/v1/sensors/${id}`);
  const hist = useApi<Prediction[]>(`/api/v1/predictions/sensors/${id}?hours=24&limit=300`);
  const rows = [...(hist.data ?? [])].reverse();
  return (
    <Card data-testid="sensor-panel">
      <Async res={sensor} height={160}>{(s) => (
        <>
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div><p className="text-xs uppercase tracking-wide text-muted">Sensor {s.sensor_id}</p><h3 className="text-lg font-semibold text-ink">{s.name ?? `Sensor ${s.sensor_id}`}</h3></div>
            <div className="flex gap-1.5"><Badge tone={statusTone(s.status)}>{s.status}</Badge>{s.tags.map((t) => <Badge key={t} tone="slate">{t}</Badge>)}</div>
          </div>
          <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-2 text-sm sm:grid-cols-4">
            {[["Rank", s.hotspot ? `#${s.hotspot.rank}` : "–"], ["Persistence", fmtPct(s.hotspot?.persistence, 0)], ["Mean congested min/day", fmtNum(s.hotspot?.mean_daily_congested_min, 0)], ["Severity (CSBI)", fmtNum(s.hotspot?.csbi, 2)],
              ["Free-flow speed", `${fmtNum(s.stats.free_flow_speed_mph, 1)} mph`], ["Congested rate (train)", fmtPct(s.stats.congested_rate as number, 1)], ["Upstream / downstream", `${s.stats.in_degree ?? "–"} / ${s.stats.out_degree ?? "–"}`], ["Location", `${s.latitude.toFixed(4)}, ${s.longitude.toFixed(4)}`]].map(([k, v]) => (
              <div key={k}><dt className="text-[11px] text-muted">{k}</dt><dd className="font-semibold text-ink">{v}</dd></div>
            ))}
          </dl>
          <p className="mt-3 text-xs"><Link className="font-medium text-lav-700 hover:underline" href={`/data?sensor=${s.sensor_id}`}>Edit this sensor in Data Management →</Link></p>
        </>
      )}</Async>
      <div className="mt-4 border-t border-lav-100 pt-3">
        {hist.data && rows.length === 0 ? <EmptyState>No stored predictions for this sensor.</EmptyState> : rows.length > 0 && (
          <PlotChart title="Predicted congestion probability (next 5 min) and measured speed" height={260} testId="sensor-history"
            data={[{ type: "scatter", mode: "lines", name: "P(congested in 5 min)", x: rows.map((r) => r.event_ts), y: rows.map((r) => r.probability), line: { color: PALETTE[0], width: 2 }, yaxis: "y" },
              { type: "scatter", mode: "lines", name: "Speed (mph)", x: rows.map((r) => r.event_ts), y: rows.map((r) => r.speed_mph), line: { color: PALETTE[3], width: 1.5, dash: "dot" }, yaxis: "y2" }]}
            layout={{ yaxis: { title: { text: "probability" }, range: [0, 1.02] }, yaxis2: { overlaying: "y", side: "right", title: { text: "mph" }, showgrid: false } }} />
        )}
      </div>
    </Card>
  );
}

export default function Page() {
  const [cls, setCls] = useState("");
  const [minP, setMinP] = useState(0);
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState("rank");
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<number | null>(null);
  const classes = useApi<ClassRow[]>("/api/v1/hotspots/classes");
  const res = useApi<Paged<Hotspot>>(`/api/v1/hotspots${qs({ limit: 500, class: cls, min_persistence: minP || undefined, sort, order: sort === "rank" || sort === "csbi_rank" ? "asc" : "desc" })}`);
  const rows = useMemo(() => (res.data?.items ?? []).filter((h) => !search || String(h.sensor_id).includes(search)), [res.data, search]);
  const columns: Column<Hotspot>[] = [
    { key: "rank", header: "Rank", align: "right", sort: (r) => r.rank },
    { key: "sensor_id", header: "Sensor", sort: (r) => r.sensor_id, render: (r) => <span className="font-semibold text-lav-800">{r.sensor_id}</span> },
    { key: "class", header: "Class", render: (r) => <Badge tone={r.is_hotspot ? "rose" : r.hotspot_class === "never" ? "slate" : "amber"}>{r.hotspot_class}</Badge> },
    { key: "persistence", header: "Persistence", align: "right", sort: (r) => r.persistence, render: (r) => fmtPct(r.persistence, 0) },
    { key: "mean", header: "Congested min/day", align: "right", sort: (r) => r.mean_daily_congested_min, render: (r) => fmtNum(r.mean_daily_congested_min, 0) },
    { key: "csbi", header: "CSBI", align: "right", sort: (r) => r.csbi, render: (r) => fmtNum(r.csbi, 2) },
    { key: "tti", header: "TTI (peak)", align: "right", sort: (r) => r.tti_peak, render: (r) => fmtNum(r.tti_peak, 2) },
  ];
  const top = rows.slice(0, 12);
  return (
    <>
      <PageHeader eyebrow="Spark Q5 · Q9" title="Congestion Hotspots" subtitle="Where congestion persists. Hotspot class and severity come from the Spark analysis (Q5 persistence, Q9 severity index) stored in MongoDB; click a sensor on the map or in the table for its details and predictions." />
      <section className="mb-5 grid gap-4 sm:grid-cols-2 xl:grid-cols-4" aria-label="Classes">
        {(classes.data ?? []).map((c) => (
          <button key={c.class} onClick={() => { setCls(cls === c.class ? "" : c.class); setOffset(0); }} className="text-left" aria-pressed={cls === c.class}>
            <StatCard label={c.class} value={fmtInt(c.sensors)} sub={`mean persistence ${fmtPct(c.mean_persistence, 0)} · ${fmtNum(c.mean_daily_congested_min, 0)} min/day`} tone={c.class === "persistent hotspot" ? "rose" : "violet"} icon={cls === c.class ? "✓" : "◉"} />
          </button>
        ))}
        {!classes.data && [0, 1, 2, 3].map((i) => <StatCard key={i} label="…" value="" loading />)}
      </section>
      <Card className="mb-5">
        <div className="grid gap-4 md:grid-cols-4">
          <Field label="Class"><Select value={cls} onChange={(e) => { setCls(e.target.value); setOffset(0); }} aria-label="Class filter"><option value="">All classes</option>{(classes.data ?? []).map((c) => <option key={c.class} value={c.class}>{c.class}</option>)}</Select></Field>
          <Field label={`Minimum persistence: ${fmtPct(minP, 0)}`}><input type="range" min={0} max={1} step={0.05} value={minP} onChange={(e) => { setMinP(Number(e.target.value)); setOffset(0); }} className="w-full accent-lav-700" aria-label="Minimum persistence" /></Field>
          <Field label="Sensor id contains"><Input value={search} onChange={(e) => { setSearch(e.target.value.replace(/\D/g, "")); setOffset(0); }} placeholder="e.g. 4005" aria-label="Sensor search" /></Field>
          <Field label="Rank by"><Select value={sort} onChange={(e) => setSort(e.target.value)} aria-label="Sort"><option value="rank">Mean congested minutes</option><option value="csbi_rank">Severity (CSBI)</option><option value="persistence">Persistence</option></Select></Field>
        </div>
      </Card>
      <div className="grid gap-5 xl:grid-cols-5">
        <Card className="xl:col-span-3">
          <Async res={res} height={420}>{() => (
            <PlotChart title={`Sensor map (${fmtInt(rows.length)} sensors) - colour: persistence, size: congested minutes per day`} height={430} testId="hotspot-map"
              onPointClick={(p) => p.customdata && setSelected(Number(p.customdata))}
              data={[{ type: "scatter", mode: "markers", x: rows.map((r) => r.longitude), y: rows.map((r) => r.latitude), customdata: rows.map((r) => r.sensor_id),
                text: rows.map((r) => `sensor ${r.sensor_id} · ${r.hotspot_class}<br>persistence ${fmtPct(r.persistence, 0)} · ${fmtNum(r.mean_daily_congested_min, 0)} min/day`),
                hovertemplate: "%{text}<extra></extra>", marker: { size: rows.map((r) => 6 + 14 * Math.sqrt((r.mean_daily_congested_min ?? 0) / 370)), color: rows.map((r) => r.persistence ?? 0), colorscale: "Portland", cmin: 0, cmax: 1, showscale: true, colorbar: { title: { text: "persistence", side: "right" }, thickness: 12, tickformat: ".0%" }, opacity: 0.85, line: { width: 0.6, color: "#fff" } } },
              ...(selected != null && rows.find((r) => r.sensor_id === selected) ? [{ type: "scatter", mode: "markers", x: [rows.find((r) => r.sensor_id === selected)!.longitude], y: [rows.find((r) => r.sensor_id === selected)!.latitude], marker: { size: 24, color: "rgba(0,0,0,0)", line: { width: 3, color: "#1f1b3d" } }, hoverinfo: "skip", showlegend: false }] : [])]}
              layout={{ xaxis: { title: { text: "Longitude" } }, yaxis: { title: { text: "Latitude" }, scaleanchor: "x" }, showlegend: false, hovermode: "closest" }} />
          )}</Async>
        </Card>
        <Card className="xl:col-span-2">
          <Async res={res} height={420}>{() => (
            <PlotChart title="Top sensors by congested minutes per working day" height={430} testId="hotspot-top"
              onPointClick={(p) => p.y != null && setSelected(Number(String(p.y).replace(/\D/g, "")))}
              data={[{ type: "bar", orientation: "h", x: [...top].reverse().map((r) => r.mean_daily_congested_min), y: [...top].reverse().map((r) => `sensor ${r.sensor_id}`), marker: { color: [...top].reverse().map((r) => (r.sensor_id === selected ? "#e8836a" : PALETTE[0])) }, hovertemplate: "%{y}: %{x:.0f} min/day<extra></extra>" }]}
              layout={{ yaxis: { type: "category" }, margin: { l: 90 }, showlegend: false, xaxis: { title: { text: "minutes per working day" } } }} />
          )}</Async>
        </Card>
      </div>
      <div className="mt-5 grid gap-5 xl:grid-cols-2">
        <Card>
          <SectionTitle title="Hotspot ranking" hint={`${fmtInt(rows.length)} sensors match the filters`} />
          <Async res={res} height={300}>{() => (<><DataTable columns={columns} rows={rows.slice(offset, offset + PAGE)} rowKey={(r) => r.sensor_id} dense onRowClick={(r) => setSelected(r.sensor_id)} selectedKey={selected} empty="No sensor matches the filters." /><Pager total={rows.length} limit={PAGE} offset={offset} onChange={setOffset} /></>)}</Async>
        </Card>
        {selected != null ? <SensorPanel key={selected} id={selected} /> : <Card><EmptyState>Select a sensor on the map or in the table to see its metadata, hotspot metrics and prediction history.</EmptyState></Card>}
      </div>
    </>
  );
}
