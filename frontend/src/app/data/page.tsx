"use client";

import { Suspense, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Card, PageHeader, Pills, SectionTitle, Skeleton, Badge } from "@/components/ui";
import { SensorsTab, type Op } from "@/components/SensorsTab";
import { SettingsTab } from "@/components/SettingsTab";
import { fmtNum } from "@/lib/format";

function OpLog({ ops }: { ops: Op[] }) {
  return (
    <Card>
      <SectionTitle title="Write log (this session)" hint="Each row is a real HTTP call to the API that changed MongoDB." />
      {ops.length === 0 ? <p className="text-sm text-muted">No writes yet. Create, edit, change a status or save a setting.</p> : (
        <ol className="space-y-1.5 text-xs" data-testid="oplog">
          {ops.slice(0, 8).map((o, i) => (
            <li key={i} className="flex flex-wrap items-center gap-2 rounded-lg bg-lav-50 px-3 py-1.5">
              <Badge tone={o.status >= 200 && o.status < 300 ? "green" : "rose"}>{o.status || "ERR"}</Badge>
              <code className="font-semibold text-lav-900">{o.method}</code><code className="text-ink">{o.path}</code>
              <span className="ml-auto text-muted">{o.note} {o.ms ? `· ${fmtNum(o.ms, 0)} ms` : ""} · {o.at.slice(11, 19)}</span>
            </li>
          ))}
        </ol>
      )}
    </Card>
  );
}

function DataInner() {
  const sp = useSearchParams();
  const focus = sp.get("sensor") && /^\d+$/.test(sp.get("sensor")!) ? Number(sp.get("sensor")) : null;
  const [tab, setTab] = useState<"sensors" | "settings">("sensors");
  const [ops, setOps] = useState<Op[]>([]);
  const log = (o: Op) => setOps((x) => [o, ...x].slice(0, 30));
  return (
    <>
      <PageHeader eyebrow="MongoDB · FastAPI CRUD" title="Data Management" subtitle="Create, edit and delete sensor metadata and analyst settings. Changes are validated by the API, written to MongoDB at once, versioned (optimistic concurrency) and survive restarts of the API, the dashboard and the database."
        right={<Pills<"sensors" | "settings"> label="Collection" value={tab} onChange={setTab} options={[{ value: "sensors", label: "Sensor metadata" }, { value: "settings", label: "Analyst settings" }]} />} />
      <div className="space-y-5">
        {tab === "sensors" ? <SensorsTab log={log} focusId={focus} /> : <SettingsTab log={log} />}
        <OpLog ops={ops} />
      </div>
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<Skeleton h={400} />}><DataInner /></Suspense>;
}
