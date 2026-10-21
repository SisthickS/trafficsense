"use client";

import { useState } from "react";
import { useSWRConfig } from "swr";
import { useApi } from "@/lib/hooks";
import { ApiError } from "@/lib/api";
import { Async, Badge, Button, Card, Field, Input, Modal, Select, TextArea, useToast } from "@/components/ui";
import { useRequestLogger, type LogFn } from "@/components/SensorsTab";
import type { Setting } from "@/lib/types";
import { fmtAgo } from "@/lib/format";

const show = (v: unknown) => (typeof v === "string" ? v : JSON.stringify(v));

function SettingRow({ s, log, onChanged }: { s: Setting; log: LogFn; onChanged: () => void }) {
  const { notify } = useToast();
  const logged = useRequestLogger(log);
  const [draft, setDraft] = useState<string>(show(s.value));
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const dirty = draft !== show(s.value);

  function parsed(): unknown {
    if (s.type === "int" || s.type === "float") { if (draft.trim() === "" || Number.isNaN(Number(draft))) throw new Error("Enter a number"); return Number(draft); }
    if (s.type === "string") return draft;
    try { return JSON.parse(draft); } catch { throw new Error("Value must be valid JSON"); }
  }
  async function save() {
    setErr(""); setBusy(true);
    try {
      const r = await logged<Setting>("PUT", `/api/v1/settings/${s.key}`, { json: { value: parsed() }, ifMatch: s.version ?? undefined, actor: "dashboard" });
      notify({ kind: "ok", title: `Saved ${s.key}`, detail: `MongoDB settings document version ${r.data.version}` });
      onChanged();
    } catch (e: any) { setErr(e instanceof ApiError ? (e.status === 412 ? "Changed elsewhere: reload." : e.readable) : e.message); }
    setBusy(false);
  }
  async function reset() {
    setBusy(true);
    try {
      await logged("DELETE", `/api/v1/settings/${s.key}`, { ifMatch: s.version ?? undefined });
      notify({ kind: "ok", title: s.known ? `${s.key} reset to default` : `${s.key} deleted`, detail: "The override was removed from MongoDB" });
      onChanged();
    } catch (e: any) { setErr(e.readable ?? e.message); }
    setBusy(false);
  }
  const input = s.choices ? (
    <Select value={draft} onChange={(e) => setDraft(e.target.value)} aria-label={s.key}>{s.choices.map((c) => <option key={c}>{c}</option>)}</Select>
  ) : s.type === "json" ? (
    <TextArea rows={2} value={draft} onChange={(e) => setDraft(e.target.value)} aria-label={s.key} />
  ) : (
    <Input type={s.type === "string" ? "text" : "number"} step={s.type === "int" ? 1 : "any"} min={s.minimum ?? undefined} max={s.maximum ?? undefined} value={draft} onChange={(e) => setDraft(e.target.value)} aria-label={s.key} />
  );
  return (
    <tr className="border-t border-lav-100 align-top" data-setting={s.key}>
      <td className="px-3 py-3"><p className="font-mono text-xs font-semibold text-lav-800">{s.key}</p><p className="mt-0.5 max-w-xs text-[11px] leading-snug text-muted">{s.description}</p><Badge tone={s.known ? "violet" : "teal"}>{s.category}</Badge></td>
      <td className="w-56 px-3 py-3">{input}{err && <p role="alert" className="mt-1 text-xs text-rose-600">{err}</p>}{s.minimum != null && <p className="mt-1 text-[11px] text-slate-400">{s.minimum} – {s.maximum}</p>}</td>
      <td className="px-3 py-3 text-xs text-muted">{s.is_default ? <Badge tone="slate">default</Badge> : <><Badge tone="green">overridden</Badge><p className="mt-1">v{s.version} · {fmtAgo(s.updated_at)}{s.updated_by && ` · ${s.updated_by}`}</p></>}{s.known && !s.is_default && <p className="mt-1">default {show(s.default)}</p>}</td>
      <td className="px-3 py-3 text-right"><span className="flex justify-end gap-1"><Button size="sm" disabled={!dirty || busy} onClick={save}>Save</Button><Button size="sm" variant="ghost" disabled={busy || s.is_default} onClick={reset}>{s.known ? "Reset" : "Delete"}</Button></span></td>
    </tr>
  );
}

export function SettingsTab({ log }: { log: LogFn }) {
  const { notify } = useToast();
  const { mutate } = useSWRConfig();
  const logged = useRequestLogger(log);
  const res = useApi<Setting[]>("/api/v1/settings");
  const [adding, setAdding] = useState(false);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("{}");
  const [desc, setDesc] = useState("");
  const [err, setErr] = useState("");
  const changed = () => mutate((k) => typeof k === "string" && k.startsWith("/api/v1/settings"));

  async function create() {
    setErr("");
    let v: unknown;
    try { v = JSON.parse(value); } catch { setErr("Value must be valid JSON, e.g. 42, \"text\" or {\"a\": 1}"); return; }
    try {
      const r = await logged<Setting>("POST", "/api/v1/settings", { json: { key, value: v, description: desc || undefined }, actor: "dashboard" });
      notify({ kind: "ok", title: `Setting ${r.data.key} created`, detail: `Stored in MongoDB · version ${r.data.version}` });
      setAdding(false); setKey(""); setValue("{}"); setDesc(""); changed();
    } catch (e: any) { setErr(e instanceof ApiError ? e.readable : String(e)); }
  }
  return (
    <>
      <Card>
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <p className="max-w-2xl text-xs text-muted">Known settings are typed and bounded and change how the API behaves (alerts, watch-list, page size, look-back). Deleting an override reverts a known setting to its default. Custom settings accept any JSON.</p>
          <Button onClick={() => setAdding(true)}>+ Add custom setting</Button>
        </div>
        <Async res={res} height={300}>{(rows) => (
          <div className="scroll-thin overflow-auto rounded-xl border border-lav-200">
            <table className="w-full border-collapse text-sm"><thead className="bg-lav-100 text-left text-[11px] uppercase tracking-wide text-lav-800"><tr><th className="px-3 py-2">Setting</th><th className="px-3 py-2">Value</th><th className="px-3 py-2">State</th><th className="px-3 py-2" /></tr></thead>
              <tbody>{rows.map((s) => <SettingRow key={`${s.key}-${s.version ?? "d"}-${show(s.value)}`} s={s} log={log} onChanged={changed} />)}</tbody></table>
          </div>
        )}</Async>
      </Card>
      <Modal open={adding} onClose={() => setAdding(false)} title="Add custom setting">
        <form onSubmit={(e) => { e.preventDefault(); create(); }} className="space-y-3">
          <Field label="Key" hint="lower-case slug, e.g. custom.map_layers"><Input value={key} onChange={(e) => setKey(e.target.value)} aria-label="Setting key" /></Field>
          <Field label="Value (JSON)"><TextArea rows={4} value={value} onChange={(e) => setValue(e.target.value)} aria-label="Setting value" /></Field>
          <Field label="Description"><Input value={desc} onChange={(e) => setDesc(e.target.value)} aria-label="Setting description" /></Field>
          {err && <p role="alert" className="rounded-lg bg-rose-50 p-2 text-sm text-rose-800">{err}</p>}
          <div className="flex justify-end gap-2"><Button type="button" variant="ghost" onClick={() => setAdding(false)}>Cancel</Button><Button type="submit">Create setting</Button></div>
        </form>
      </Modal>
    </>
  );
}
