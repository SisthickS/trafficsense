"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { ApiError } from "@/lib/api";

export const cx = (...a: (string | false | null | undefined)[]) => a.filter(Boolean).join(" ");

/* ------------------------------------------------------------------ surfaces */
export function Card({ children, className, pad = true, ...rest }: { children: ReactNode; className?: string; pad?: boolean } & React.HTMLAttributes<HTMLElement>) {
  return <section {...rest} className={cx("rounded-2xl border border-lav-200 bg-white shadow-soft", pad && "p-5", className)}>{children}</section>;
}

export function SectionTitle({ title, hint, right }: { title: ReactNode; hint?: ReactNode; right?: ReactNode }) {
  return (
    <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
      <div>
        <h2 className="text-base font-semibold tracking-tight text-ink">{title}</h2>
        {hint && <p className="mt-0.5 text-xs text-muted">{hint}</p>}
      </div>
      {right}
    </div>
  );
}

export function PageHeader({ eyebrow, title, subtitle, right }: { eyebrow?: string; title: string; subtitle?: ReactNode; right?: ReactNode }) {
  return (
    <header className="mb-6 flex flex-wrap items-end justify-between gap-4">
      <div className="max-w-3xl">
        {eyebrow && <p className="text-xs font-semibold uppercase tracking-[0.14em] text-lav-700">{eyebrow}</p>}
        <h1 className="mt-1 text-2xl font-semibold tracking-tight text-ink sm:text-3xl">{title}</h1>
        {subtitle && <p className="mt-2 text-sm leading-relaxed text-muted">{subtitle}</p>}
      </div>
      {right}
    </header>
  );
}

type Tone = "violet" | "teal" | "amber" | "rose" | "slate" | "green";
const TONES: Record<Tone, string> = {
  violet: "bg-lav-100 text-lav-800 ring-lav-200",
  teal: "bg-teal-50 text-teal-800 ring-teal-200",
  amber: "bg-amber-50 text-amber-800 ring-amber-200",
  rose: "bg-rose-50 text-rose-800 ring-rose-200",
  slate: "bg-slate-100 text-slate-700 ring-slate-200",
  green: "bg-emerald-50 text-emerald-800 ring-emerald-200",
};

export function Badge({ children, tone = "violet", title }: { children: ReactNode; tone?: Tone; title?: string }) {
  return <span title={title} className={cx("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium ring-1 ring-inset", TONES[tone])}>{children}</span>;
}

export const statusTone = (s: string): Tone =>
  ({ succeeded: "green", active: "green", ok: "green", ready: "green", failed: "rose", decommissioned: "rose", maintenance: "amber", inactive: "slate", running: "violet" } as Record<string, Tone>)[s] ?? "slate";

export function StatCard({ label, value, sub, icon, tone = "violet", loading }: { label: string; value: ReactNode; sub?: ReactNode; icon?: ReactNode; tone?: Tone; loading?: boolean }) {
  return (
    <Card className="relative overflow-hidden">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs font-medium uppercase tracking-wide text-muted">{label}</p>
          {loading ? <div className="mt-2 h-8 w-24 animate-pulse rounded-lg bg-lav-100" /> : <p className="mt-1 truncate text-2xl font-semibold tracking-tight text-ink" data-testid={`stat-${label}`}>{value}</p>}
          {sub && <p className="mt-1 text-xs text-muted">{sub}</p>}
        </div>
        {icon && <span className={cx("grid h-9 w-9 shrink-0 place-items-center rounded-xl text-base ring-1 ring-inset", TONES[tone])}>{icon}</span>}
      </div>
    </Card>
  );
}

/* ------------------------------------------------------------------ controls */
export function Button({ children, variant = "primary", size = "md", className, ...rest }: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "soft" | "ghost" | "danger"; size?: "sm" | "md" }) {
  const v = {
    primary: "bg-lav-700 text-white hover:bg-lav-800 shadow-sm disabled:bg-lav-300",
    soft: "bg-lav-100 text-lav-800 hover:bg-lav-200 disabled:opacity-50",
    ghost: "text-lav-800 hover:bg-lav-100 disabled:opacity-50",
    danger: "bg-rose-600 text-white hover:bg-rose-700 disabled:bg-rose-300",
  }[variant];
  return (
    <button {...rest} className={cx("inline-flex items-center justify-center gap-1.5 rounded-xl font-medium transition focus:outline-none focus-visible:ring-2 focus-visible:ring-lav-500 disabled:cursor-not-allowed", size === "sm" ? "px-2.5 py-1 text-xs" : "px-3.5 py-2 text-sm", v, className)}>
      {children}
    </button>
  );
}

const fieldCls = "w-full rounded-xl border border-lav-200 bg-white px-3 py-2 text-sm text-ink placeholder:text-slate-400 focus:border-lav-500 focus:outline-none focus:ring-2 focus:ring-lav-200 disabled:bg-lav-50";

export function Field({ label, hint, error, children }: { label: string; hint?: string; error?: string; children: ReactNode }) {
  return (
    <label className="block text-sm">
      <span className="mb-1 block text-xs font-medium text-muted">{label}</span>
      {children}
      {error ? <span className="mt-1 block text-xs text-rose-600" role="alert">{error}</span> : hint ? <span className="mt-1 block text-[11px] text-slate-400">{hint}</span> : null}
    </label>
  );
}

export const Input = (p: React.InputHTMLAttributes<HTMLInputElement>) => <input {...p} className={cx(fieldCls, p.className)} />;
export const Select = (p: React.SelectHTMLAttributes<HTMLSelectElement>) => <select {...p} className={cx(fieldCls, "pr-8", p.className)} />;
export const TextArea = (p: React.TextareaHTMLAttributes<HTMLTextAreaElement>) => <textarea {...p} className={cx(fieldCls, "font-mono text-xs", p.className)} />;

export function Pills<T extends string>({ options, value, onChange, label }: { options: { value: T; label: string; hint?: string }[]; value: T; onChange: (v: T) => void; label?: string }) {
  return (
    <div role="tablist" aria-label={label} className="flex flex-wrap gap-1.5">
      {options.map((o) => (
        <button key={o.value} role="tab" aria-selected={o.value === value} title={o.hint} onClick={() => onChange(o.value)}
          className={cx("rounded-full px-3 py-1 text-xs font-medium ring-1 ring-inset transition", o.value === value ? "bg-lav-700 text-white ring-lav-700" : "bg-white text-lav-800 ring-lav-200 hover:bg-lav-100")}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ states */
export function Spinner({ label = "Loading" }: { label?: string }) {
  return <div className="flex items-center gap-2 py-6 text-sm text-muted" role="status"><span className="h-4 w-4 animate-spin rounded-full border-2 border-lav-300 border-t-lav-700" />{label}…</div>;
}

export function Skeleton({ h = 240 }: { h?: number }) {
  return <div className="animate-pulse rounded-xl bg-lav-100" style={{ height: h }} aria-hidden />;
}

export function ErrorState({ error, retry }: { error: unknown; retry?: () => void }) {
  const e = error instanceof ApiError ? error : null;
  const down = e?.status === 502 || e?.status === 503 || (!e && error instanceof TypeError);
  return (
    <div role="alert" className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-900">
      <p className="font-semibold">{down ? "Backend unavailable" : "Could not load this data"}</p>
      <p className="mt-1 text-rose-800/90">{e ? e.readable : String((error as Error)?.message ?? error)}</p>
      {e?.requestId && <p className="mt-1 text-[11px] text-rose-700/70">request {e.requestId}</p>}
      {retry && <Button variant="soft" size="sm" className="mt-3" onClick={retry}>Retry</Button>}
    </div>
  );
}

export function EmptyState({ children }: { children: ReactNode }) {
  return <div className="rounded-xl border border-dashed border-lav-300 bg-lav-50 p-6 text-center text-sm text-muted">{children}</div>;
}

/** Renders loading / error / data for one SWR result. */
export function Async<T>({ res, children, height = 200 }: { res: { data?: T; error?: unknown; isLoading: boolean; mutate?: () => unknown }; children: (d: T) => ReactNode; height?: number }) {
  if (res.error && res.data === undefined) return <ErrorState error={res.error} retry={() => res.mutate?.()} />;
  if (res.data === undefined) return <Skeleton h={height} />;
  return <>{children(res.data)}</>;
}

/* ------------------------------------------------------------------ modal + toast */
export function Modal({ open, onClose, title, children, wide }: { open: boolean; onClose: () => void; title: string; children: ReactNode; wide?: boolean }) {
  useEffect(() => {
    if (!open) return;
    const h = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-lav-900/40 p-4 backdrop-blur-[2px]" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div role="dialog" aria-modal="true" aria-label={title} className={cx("max-h-[90vh] w-full overflow-y-auto rounded-2xl bg-white p-6 shadow-2xl", wide ? "max-w-3xl" : "max-w-lg")}>
        <div className="mb-4 flex items-start justify-between gap-4">
          <h3 className="text-lg font-semibold text-ink">{title}</h3>
          <button onClick={onClose} aria-label="Close" className="rounded-lg px-2 text-xl leading-none text-muted hover:bg-lav-100">×</button>
        </div>
        {children}
      </div>
    </div>
  );
}

interface Toast { id: number; kind: "ok" | "error"; title: string; detail?: string }
const ToastCtx = createContext<{ notify: (t: Omit<Toast, "id">) => void }>({ notify: () => {} });
export const useToast = () => useContext(ToastCtx);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<Toast[]>([]);
  const notify = useCallback((t: Omit<Toast, "id">) => {
    const id = Date.now() + Math.random();
    setItems((x) => [...x.slice(-3), { ...t, id }]);
    setTimeout(() => setItems((x) => x.filter((i) => i.id !== id)), t.kind === "error" ? 9000 : 4500);
  }, []);
  const value = useMemo(() => ({ notify }), [notify]);
  return (
    <ToastCtx.Provider value={value}>
      {children}
      <div className="pointer-events-none fixed bottom-4 right-4 z-[60] flex w-[min(92vw,380px)] flex-col gap-2" aria-live="polite">
        {items.map((t) => (
          <div key={t.id} role="status" data-testid="toast" className={cx("pointer-events-auto rounded-xl border p-3 text-sm shadow-lg", t.kind === "ok" ? "border-emerald-200 bg-emerald-50 text-emerald-900" : "border-rose-200 bg-rose-50 text-rose-900")}>
            <p className="font-semibold">{t.title}</p>
            {t.detail && <p className="mt-0.5 text-xs opacity-80">{t.detail}</p>}
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}

/* ------------------------------------------------------------------ table */
export interface Column<T> {
  key: string; header: ReactNode; render?: (row: T) => ReactNode; sort?: (row: T) => number | string | null | undefined; align?: "left" | "right"; className?: string;
}

export function DataTable<T>({ columns, rows, rowKey, onRowClick, selectedKey, dense, maxHeight, empty, initialSort }: {
  columns: Column<T>[]; rows: T[]; rowKey: (r: T) => string | number; onRowClick?: (r: T) => void; selectedKey?: string | number | null; dense?: boolean;
  maxHeight?: number; empty?: ReactNode; initialSort?: { key: string; dir: "asc" | "desc" };
}) {
  const [sort, setSort] = useState(initialSort ?? null);
  const sorted = useMemo(() => {
    const col = columns.find((c) => c.key === sort?.key);
    if (!col?.sort || !sort) return rows;
    const f = col.sort;
    return [...rows].sort((a, b) => {
      const x = f(a), y = f(b);
      if (x == null) return 1;
      if (y == null) return -1;
      return (x < y ? -1 : x > y ? 1 : 0) * (sort.dir === "asc" ? 1 : -1);
    });
  }, [rows, sort, columns]);
  if (!rows.length) return <EmptyState>{empty ?? "No rows."}</EmptyState>;
  return (
    <div className="scroll-thin overflow-auto rounded-xl border border-lav-200" style={maxHeight ? { maxHeight } : undefined}>
      <table className="w-full border-collapse text-sm">
        <thead className="sticky top-0 z-10 bg-lav-100 text-left text-[11px] uppercase tracking-wide text-lav-800">
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col" className={cx("whitespace-nowrap px-3 py-2 font-semibold", c.align === "right" && "text-right", c.sort && "cursor-pointer select-none hover:bg-lav-200")}
                onClick={() => c.sort && setSort((s) => (s?.key === c.key ? { key: c.key, dir: s.dir === "asc" ? "desc" : "asc" } : { key: c.key, dir: "asc" }))}
                aria-sort={sort?.key === c.key ? (sort.dir === "asc" ? "ascending" : "descending") : undefined}>
                {c.header}{sort?.key === c.key && <span aria-hidden> {sort.dir === "asc" ? "▲" : "▼"}</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => {
            const k = rowKey(r);
            return (
              <tr key={k} onClick={() => onRowClick?.(r)} data-row={k}
                className={cx("border-t border-lav-100 bg-white", onRowClick && "cursor-pointer hover:bg-lav-50", selectedKey === k && "bg-lav-100")}>
                {columns.map((c) => (
                  <td key={c.key} className={cx(dense ? "px-3 py-1.5" : "px-3 py-2.5", c.align === "right" && "text-right tabular-nums", c.className)}>
                    {c.render ? c.render(r) : String((r as Record<string, unknown>)[c.key] ?? "–")}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function Pager({ total, limit, offset, onChange }: { total: number; limit: number; offset: number; onChange: (offset: number) => void }) {
  const page = Math.floor(offset / limit) + 1, pages = Math.max(1, Math.ceil(total / limit));
  return (
    <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-muted">
      <span data-testid="pager-range">{total === 0 ? "0 rows" : `${offset + 1}–${Math.min(offset + limit, total)} of ${total.toLocaleString("en-US")}`}</span>
      <div className="flex items-center gap-1.5">
        <Button variant="soft" size="sm" disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - limit))}>Previous</Button>
        <span>page {page} / {pages}</span>
        <Button variant="soft" size="sm" disabled={offset + limit >= total} onClick={() => onChange(offset + limit)}>Next</Button>
      </div>
    </div>
  );
}
