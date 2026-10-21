"use client";

import { useEffect, useState } from "react";
import { useSWRConfig } from "swr";
import { useApi } from "@/lib/hooks";
import { ApiError, qs, request } from "@/lib/api";
import { Async, Badge, Button, Card, DataTable, Field, Input, Modal, Pager, Select, TextArea, statusTone, useToast, type Column } from "@/components/ui";
import type { Paged, Sensor } from "@/lib/types";
import { fmtAgo, fmtNum } from "@/lib/format";

export interface Op { at: string; method: string; path: string; status: number; ms: number; note: string }
export type LogFn = (op: Op) => void;

const STATUSES = ["active", "inactive", "maintenance", "decommissioned"] as const;
const PAGE = 10;

interface FormState { sensor_id: string; latitude: string; longitude: string; name: string; description: string; direction: string; lanes: string; status: string; tags: string }
const EMPTY: FormState = { sensor_id: "", latitude: "", longitude: "", name: "", description: "", direction: "", lanes: "", status: "active", tags: "" };
const fromSensor = (s: Sensor): FormState => ({ sensor_id: String(s.sensor_id), latitude: String(s.latitude), longitude: String(s.longitude), name: s.name ?? "", description: s.description ?? "", direction: s.direction ?? "", lanes: s.lanes == null ? "" : String(s.lanes), status: s.status, tags: s.tags.join(", ") });

/** Field-level errors from the API (`body.latitude` -> latitude). */
function fieldErrors(e: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  if (e instanceof ApiError) for (const d of e.details) if (d.field) out[d.field.replace(/^body\./, "").split(".")[0]] = d.message ?? "invalid";
  return out;
}

export function useRequestLogger(log: LogFn) {
  return async function logged<T>(method: "POST" | "PUT" | "PATCH" | "DELETE" | "GET", path: string, opts: Parameters<typeof request>[1] = {}) {
    try {
      const r = await request<T>(path, { ...opts, method });
      log({ at: new Date().toISOString(), method, path, status: r.status, ms: r.ms, note: r.etag ? `version ${r.etag}` : "" });
      return r;
    } catch (e) {
      log({ at: new Date().toISOString(), method, path, status: e instanceof ApiError ? e.status : 0, ms: 0, note: e instanceof ApiError ? e.code : "network error" });
      throw e;
    }
  };
}

function SensorModal({ open, sensor, onClose, onSaved, log }: { open: boolean; sensor: Sensor | null; onClose: () => void; onSaved: () => void; log: LogFn }) {
  const { notify } = useToast();
  const logged = useRequestLogger(log);
  const [f, setF] = useState<FormState>(EMPTY);
  const [errs, setErrs] = useState<Record<string, string>>({});
  const [general, setGeneral] = useState("");
  const [busy, setBusy] = useState(false);
  useEffect(() => { if (open) { setF(sensor ? fromSensor(sensor) : EMPTY); setErrs({}); setGeneral(""); } }, [open, sensor]);
  const set = (k: keyof FormState) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => setF({ ...f, [k]: e.target.value });

  function payload(): Record<string, unknown> {
    const tags = f.tags.split(",").map((t) => t.trim()).filter(Boolean);
    return {
      latitude: f.latitude === "" ? undefined : Number(f.latitude), longitude: f.longitude === "" ? undefined : Number(f.longitude),
      name: f.name.trim() || null, description: f.description.trim() || null, direction: f.direction || null, lanes: f.lanes === "" ? null : Number(f.lanes), status: f.status, tags,
    };
  }
  async function submit() {
    setBusy(true); setErrs({}); setGeneral("");
    try {
      if (!sensor) {
        const r = await logged<Sensor>("POST", "/api/v1/sensors", { json: { sensor_id: f.sensor_id === "" ? undefined : Number(f.sensor_id), ...payload() } });
        notify({ kind: "ok", title: `Sensor ${r.data.sensor_id} created`, detail: `Stored in MongoDB · version ${r.data.version}` });
      } else {
        const body = payload();
        const orig = fromSensor(sensor);
        const changed = Object.fromEntries(Object.entries(body).filter(([k]) => (k === "tags" ? f.tags !== orig.tags : (f as any)[k] !== (orig as any)[k])));
        if (!Object.keys(changed).length) { onClose(); setBusy(false); return; }
        const r = await logged<Sensor>("PATCH", `/api/v1/sensors/${sensor.sensor_id}`, { json: changed, ifMatch: sensor.version });
        notify({ kind: "ok", title: `Sensor ${sensor.sensor_id} updated`, detail: `Fields: ${Object.keys(changed).join(", ")} · MongoDB version ${r.data.version}` });
      }
      onSaved(); onClose();
    } catch (e) {
      setErrs(fieldErrors(e));
      setGeneral(e instanceof ApiError ? (e.status === 412 ? "This sensor was changed by someone else since you opened it. Close the dialog and reopen it to see the latest version." : e.status === 409 ? e.message : e.status === 422 ? "Please fix the highlighted fields." : e.readable) : "Network error");
    }
    setBusy(false);
  }
  return (
    <Modal open={open} onClose={onClose} title={sensor ? `Edit sensor ${sensor.sensor_id}` : "Add sensor"}>
      <form onSubmit={(e) => { e.preventDefault(); submit(); }} className="space-y-3" noValidate>
        {!sensor && <Field label="Sensor id" error={errs.sensor_id} hint="positive integer, unique"><Input inputMode="numeric" value={f.sensor_id} onChange={set("sensor_id")} aria-label="Sensor id" required /></Field>}
        <div className="grid grid-cols-2 gap-3">
          <Field label="Latitude" error={errs.latitude}><Input inputMode="decimal" value={f.latitude} onChange={set("latitude")} aria-label="Latitude" /></Field>
          <Field label="Longitude" error={errs.longitude}><Input inputMode="decimal" value={f.longitude} onChange={set("longitude")} aria-label="Longitude" /></Field>
        </div>
        <Field label="Name" error={errs.name}><Input value={f.name} onChange={set("name")} maxLength={120} aria-label="Name" /></Field>
        <Field label="Description" error={errs.description}><TextArea rows={2} value={f.description} onChange={set("description")} aria-label="Description" /></Field>
        <div className="grid grid-cols-3 gap-3">
          <Field label="Status" error={errs.status}><Select value={f.status} onChange={set("status")} aria-label="Status">{STATUSES.map((s) => <option key={s}>{s}</option>)}</Select></Field>
          <Field label="Direction" error={errs.direction}><Select value={f.direction} onChange={set("direction")} aria-label="Direction"><option value="">–</option>{["N", "S", "E", "W"].map((s) => <option key={s}>{s}</option>)}</Select></Field>
          <Field label="Lanes" error={errs.lanes}><Input inputMode="numeric" value={f.lanes} onChange={set("lanes")} aria-label="Lanes" /></Field>
        </div>
        <Field label="Tags" error={errs.tags} hint="comma separated, e.g. bridge, watch (letters, digits, - _)"><Input value={f.tags} onChange={set("tags")} aria-label="Tags" /></Field>
        {general && <p role="alert" className="rounded-lg bg-rose-50 p-2 text-sm text-rose-800">{general}</p>}
        <div className="flex justify-end gap-2 pt-2">
          <Button type="button" variant="ghost" onClick={onClose}>Cancel</Button>
          <Button type="submit" disabled={busy}>{busy ? "Saving…" : sensor ? "Save changes" : "Create sensor"}</Button>
        </div>
      </form>
    </Modal>
  );
}

export function SensorsTab({ log, focusId }: { log: LogFn; focusId: number | null }) {
  const { notify } = useToast();
  const { mutate } = useSWRConfig();
  const logged = useRequestLogger(log);
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [tag, setTag] = useState("");
  const [source, setSource] = useState("");
  const [sort, setSort] = useState("sensor_id");
  const [order, setOrder] = useState<"asc" | "desc">("asc");
  const [offset, setOffset] = useState(0);
  const [editing, setEditing] = useState<Sensor | "new" | null>(null);
  const [deleting, setDeleting] = useState<Sensor | null>(null);
  const [busyId, setBusyId] = useState<number | null>(null);
  const key = `/api/v1/sensors${qs({ limit: PAGE, offset, q, status, tag, source, sort, order })}`;
  const res = useApi<Paged<Sensor>>(key);
  const focus = useApi<Sensor>(focusId ? `/api/v1/sensors/${focusId}` : null);
  useEffect(() => { if (focus.data) setEditing(focus.data); }, [focus.data]);
  const refresh = () => mutate((k) => typeof k === "string" && (k.startsWith("/api/v1/sensors") || k.startsWith("/api/v1/status")));

  async function setStatusInline(s: Sensor, next: string) {
    setBusyId(s.sensor_id);
    try {
      const r = await logged<Sensor>("PATCH", `/api/v1/sensors/${s.sensor_id}`, { json: { status: next }, ifMatch: s.version });
      notify({ kind: "ok", title: `Sensor ${s.sensor_id}: ${next}`, detail: `Persisted in MongoDB · version ${r.data.version}` });
    } catch (e: any) { notify({ kind: "error", title: `Could not change sensor ${s.sensor_id}`, detail: e.status === 412 ? "Changed elsewhere; the table was refreshed." : e.readable ?? String(e) }); }
    await refresh(); setBusyId(null);
  }
  async function remove(s: Sensor) {
    try {
      await logged("DELETE", `/api/v1/sensors/${s.sensor_id}`, { ifMatch: s.version });
      notify({ kind: "ok", title: `Sensor ${s.sensor_id} deleted`, detail: "The document was removed from MongoDB" });
    } catch (e: any) { notify({ kind: "error", title: "Delete failed", detail: e.status === 412 ? "Changed elsewhere; reload and try again." : e.readable ?? String(e) }); }
    setDeleting(null); refresh();
  }

  const cols: Column<Sensor>[] = [
    { key: "sensor_id", header: "ID", sort: (s) => s.sensor_id, render: (s) => <span className="font-semibold text-lav-800">{s.sensor_id}</span> },
    { key: "name", header: "Name", render: (s) => <span title={s.description ?? ""}>{s.name ?? "–"}</span> },
    { key: "status", header: "Status", render: (s) => (
      <select aria-label={`Status of sensor ${s.sensor_id}`} value={s.status} disabled={busyId === s.sensor_id} onClick={(e) => e.stopPropagation()} onChange={(e) => setStatusInline(s, e.target.value)}
        className="rounded-lg border border-lav-200 bg-white px-2 py-1 text-xs focus:outline-none focus:ring-2 focus:ring-lav-300">{STATUSES.map((x) => <option key={x}>{x}</option>)}</select>) },
    { key: "dir", header: "Dir / lanes", render: (s) => `${s.direction ?? "–"} / ${s.lanes ?? "–"}` },
    { key: "tags", header: "Tags", render: (s) => <span className="flex flex-wrap gap-1">{s.tags.length ? s.tags.map((t) => <Badge key={t} tone="slate">{t}</Badge>) : "–"}</span> },
    { key: "loc", header: "Location", render: (s) => `${fmtNum(s.latitude, 4)}, ${fmtNum(s.longitude, 4)}` },
    { key: "src", header: "Source", render: (s) => <Badge tone={s.source === "api" ? "teal" : "violet"}>{s.source}</Badge> },
    { key: "ver", header: "Ver", align: "right", render: (s) => `v${s.version}` },
    { key: "upd", header: "Updated", render: (s) => <span title={s.updated_at}>{fmtAgo(s.updated_at)}</span> },
    { key: "act", header: "", align: "right", render: (s) => (
      <span className="flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
        <Button size="sm" variant="soft" onClick={() => setEditing(s)} aria-label={`Edit sensor ${s.sensor_id}`}>Edit</Button>
        <Button size="sm" variant="ghost" onClick={() => setDeleting(s)} aria-label={`Delete sensor ${s.sensor_id}`}>Delete</Button>
      </span>) },
  ];
  const reset = <T,>(set: (v: T) => void) => (v: T) => { set(v); setOffset(0); };
  return (
    <>
      <Card>
        <div className="mb-4 grid gap-3 md:grid-cols-6">
          <div className="md:col-span-2"><Field label="Search name, description or id"><Input value={q} onChange={(e) => reset(setQ)(e.target.value)} placeholder="e.g. 4005 or bridge" aria-label="Search sensors" /></Field></div>
          <Field label="Status"><Select value={status} onChange={(e) => reset(setStatus)(e.target.value)} aria-label="Status filter"><option value="">All</option>{STATUSES.map((s) => <option key={s}>{s}</option>)}</Select></Field>
          <Field label="Source"><Select value={source} onChange={(e) => reset(setSource)(e.target.value)} aria-label="Source filter"><option value="">All</option><option value="dataset">dataset</option><option value="api">api (created here)</option></Select></Field>
          <Field label="Tag"><Input value={tag} onChange={(e) => reset(setTag)(e.target.value.toLowerCase())} aria-label="Tag filter" /></Field>
          <Field label="Sort"><div className="flex gap-1"><Select value={sort} onChange={(e) => reset(setSort)(e.target.value)} aria-label="Sort by"><option value="sensor_id">ID</option><option value="name">Name</option><option value="status">Status</option><option value="updated_at">Updated</option></Select>
            <Button variant="soft" onClick={() => setOrder(order === "asc" ? "desc" : "asc")} aria-label="Toggle order" title={order}>{order === "asc" ? "↑" : "↓"}</Button></div></Field>
        </div>
        <div className="mb-3 flex items-center justify-between">
          <p className="text-xs text-muted">Every action below is sent to the API and written to the MongoDB <code>sensor_metadata</code> collection immediately.</p>
          <Button onClick={() => setEditing("new")}>+ Add sensor</Button>
        </div>
        <Async res={res} height={380}>{(d) => (
          <>
            <DataTable columns={cols} rows={d.items} rowKey={(s) => s.sensor_id} dense empty="No sensor matches the filters." />
            <Pager total={d.total} limit={d.limit} offset={d.offset} onChange={setOffset} />
          </>
        )}</Async>
      </Card>
      <SensorModal open={editing !== null} sensor={editing === "new" ? null : editing} onClose={() => setEditing(null)} onSaved={refresh} log={log} />
      <Modal open={deleting !== null} onClose={() => setDeleting(null)} title="Delete sensor?">
        <p className="text-sm text-muted">Sensor <strong className="text-ink">{deleting?.sensor_id}</strong> ({deleting?.name ?? "unnamed"}) will be removed from MongoDB. {deleting?.source === "dataset" && "It is a dataset sensor: its Spark results stay, and the next data load will re-create it with default metadata."}</p>
        <div className="mt-4 flex justify-end gap-2"><Button variant="ghost" onClick={() => setDeleting(null)}>Cancel</Button><Button variant="danger" onClick={() => deleting && remove(deleting)}>Delete</Button></div>
      </Modal>
    </>
  );
}
