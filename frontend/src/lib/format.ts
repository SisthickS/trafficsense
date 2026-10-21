export const fmtInt = (n: number | null | undefined) => (n == null || Number.isNaN(n) ? "–" : Math.round(n).toLocaleString("en-US"));

export const fmtNum = (n: number | null | undefined, digits = 2) =>
  n == null || Number.isNaN(n) ? "–" : n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });

export const fmtPct = (x: number | null | undefined, digits = 1) => (x == null || Number.isNaN(x) ? "–" : `${(x * 100).toFixed(digits)}%`);

export function fmtCompact(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "–";
  const a = Math.abs(n);
  if (a >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (a >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (a >= 1e4) return `${(n / 1e3).toFixed(0)}k`;
  return Math.round(n).toLocaleString("en-US");
}

export function fmtDate(iso: string | null | undefined, withTime = true): string {
  if (!iso) return "–";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("en-GB", { year: "numeric", month: "short", day: "2-digit", ...(withTime ? { hour: "2-digit", minute: "2-digit" } : {}), timeZone: "UTC" }) + (withTime ? " UTC" : "");
}

export function fmtAgo(iso: string | null | undefined): string {
  if (!iso) return "–";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 0) return "just now";
  if (s < 90) return `${Math.round(s)} s ago`;
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  if (s < 129600) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} d ago`;
}

export const fmtSeconds = (s: number | null | undefined) =>
  s == null ? "–" : s < 1 ? `${Math.round(s * 1000)} ms` : s < 120 ? `${s.toFixed(1)} s` : `${(s / 60).toFixed(1)} min`;

export const titleCase = (s: string) => s.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

/** Findings use **bold** and `code` only; escape everything else. */
export function md(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\*\*(.+?)\*\*/g, "<strong class='text-ink'>$1</strong>").replace(/`(.+?)`/g, "<code class='rounded bg-lav-100 px-1 text-[0.85em]'>$1</code>");
}

export const PALETTE = ["#6a5acd", "#2ea8a1", "#e8836a", "#d9a441", "#4c8bf5", "#c25ba6", "#7fae5a", "#8a8fa8"];
