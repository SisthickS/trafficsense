"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { useApi } from "@/lib/hooks";
import { cx } from "@/components/ui";
import type { StatusOverview, Question, TableData } from "@/lib/types";
import { fmtCompact, fmtDate } from "@/lib/format";

export const NAV = [
  { href: "/", label: "Dashboard" },
  { href: "/analytics", label: "Traffic Analytics" },
  { href: "/hotspots", label: "Hotspots" },
  { href: "/live", label: "Live Traffic" },
  { href: "/models", label: "Model Evaluation" },
  { href: "/performance", label: "Spark Performance" },
  { href: "/data", label: "Data Management" },
  { href: "/status", label: "History & Status" },
];

function ApiDot() {
  const { data, error } = useApi<{ status: string; database: string }>("/ready", { refreshMs: 15000 });
  const ok = !!data && !error;
  return (
    <span className="hidden items-center gap-2 rounded-full bg-lav-50 px-3 py-1 text-xs text-lav-900 ring-1 ring-inset ring-lav-200 md:inline-flex" title={ok ? `API ready, MongoDB database "${data?.database}"` : "API or MongoDB not reachable"} data-testid="api-status">
      <span className={cx("h-2 w-2 rounded-full", ok ? "bg-emerald-500" : error ? "bg-rose-500" : "bg-amber-400 ts-pulse")} />
      {ok ? `API · ${data?.database}` : error ? "API offline" : "connecting"}
    </span>
  );
}

export function TopNav() {
  const path = usePathname();
  const [open, setOpen] = useState(false);
  const active = (href: string) => (href === "/" ? path === "/" : path.startsWith(href));
  return (
    <header className="sticky top-0 z-40 bg-white/90 shadow-nav backdrop-blur">
      <div className="mx-auto flex max-w-[1400px] items-center gap-4 px-4 py-2.5 sm:px-6">
        <Link href="/" className="flex shrink-0 items-center gap-2.5" aria-label="TrafficSense home">
          <span className="grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-lav-600 to-lav-800 text-white shadow-sm">
            <svg viewBox="0 0 24 24" className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden><path d="M3 17l5-6 4 3 5-8 4 5" /><circle cx="20" cy="9" r="1" /></svg>
          </span>
          <span className="text-base font-semibold tracking-tight text-ink">Traffic<span className="text-lav-700">Sense</span></span>
        </Link>
        <nav aria-label="Main" className="scroll-thin hidden min-w-0 flex-1 items-center gap-0.5 overflow-x-auto lg:flex">
          {NAV.map((n) => (
            <Link key={n.href} href={n.href} aria-current={active(n.href) ? "page" : undefined}
              className={cx("whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium transition", active(n.href) ? "bg-lav-100 text-lav-800" : "text-muted hover:bg-lav-50 hover:text-lav-800")}>
              {n.label}
            </Link>
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-2">
          <ApiDot />
          <button className="rounded-lg p-2 text-lav-800 hover:bg-lav-100 lg:hidden" aria-label="Menu" aria-expanded={open} onClick={() => setOpen((o) => !o)}>
            <svg viewBox="0 0 24 24" className="h-5 w-5" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d={open ? "M6 6l12 12M18 6L6 18" : "M4 7h16M4 12h16M4 17h16"} /></svg>
          </button>
        </div>
      </div>
      {open && (
        <nav aria-label="Mobile" className="border-t border-lav-100 bg-white px-4 pb-3 lg:hidden">
          {NAV.map((n) => (
            <Link key={n.href} href={n.href} onClick={() => setOpen(false)} aria-current={active(n.href) ? "page" : undefined}
              className={cx("block rounded-lg px-3 py-2 text-sm font-medium", active(n.href) ? "bg-lav-100 text-lav-800" : "text-muted")}>
              {n.label}
            </Link>
          ))}
        </nav>
      )}
    </header>
  );
}

/** Project scope: every figure is read from the API (MongoDB), only the labels are static. */
export function ScopeStrip() {
  const status = useApi<StatusOverview>("/api/v1/status", { refreshMs: 60000 });
  const questions = useApi<Question[]>("/api/v1/analytics/questions");
  const quality = useApi<TableData>("/api/v1/analytics/questions/q13/tables/quality?limit=10");
  const daily = useApi<TableData>("/api/v1/analytics/questions/q1/tables/daily?limit=500");
  const readings = quality.data?.rows.reduce((s, r) => s + Number(r.readings ?? 0), 0);
  const days = daily.data?.rows.map((r) => String(r.date_local)).sort();
  const items: [string, string][] = [
    ["Dataset", "PEMS-BAY · SF Bay Area freeways"],
    ["Sensors", status.data ? String(status.data.counts.congestion_hotspots) : "…"],
    ["Readings", readings ? fmtCompact(readings) : "…"],
    ["Period", days?.length ? `${fmtDate(days[0], false)} → ${fmtDate(days[days.length - 1], false)}` : "…"],
    ["Resolution", "5 min"],
    ["Questions", questions.data ? String(questions.data.length) : "…"],
    ["Stack", "Spark · Kafka · MongoDB · FastAPI · Next.js"],
  ];
  return (
    <div className="border-b border-lav-200 bg-lav-100/70" data-testid="scope-strip">
      <div className="scroll-thin mx-auto flex max-w-[1400px] items-center gap-x-6 gap-y-1 overflow-x-auto px-4 py-2 text-xs sm:px-6">
        <span className="shrink-0 font-semibold uppercase tracking-[0.12em] text-lav-800">Project scope</span>
        {items.map(([k, v]) => (
          <span key={k} className="shrink-0 whitespace-nowrap text-muted"><span className="mr-1">{k}</span><strong className="font-semibold text-ink">{v}</strong></span>
        ))}
      </div>
    </div>
  );
}

export function Footer() {
  return (
    <footer className="mx-auto mt-12 max-w-[1400px] px-4 pb-10 text-xs text-muted sm:px-6">
      <div className="border-t border-lav-200 pt-4">
        TrafficSense · 23CSE352 Big Data Analytics · data and every chart are served live by the FastAPI backend from MongoDB.
      </div>
    </footer>
  );
}
