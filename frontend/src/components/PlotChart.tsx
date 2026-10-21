"use client";

import dynamic from "next/dynamic";
import { useMemo } from "react";
import { PALETTE } from "@/lib/format";
import { Skeleton } from "@/components/ui";

const Plot = dynamic(() => import("@/components/PlotlyBase"), { ssr: false, loading: () => <Skeleton h={260} /> });

export type PlotData = Record<string, any>;

/** App-wide Plotly look: transparent background, lavender grid, app font and colour way. */
export function baseLayout(extra: Record<string, any> = {}): Record<string, any> {
  const axis = { gridcolor: "#eceafb", zerolinecolor: "#d6d0f7", linecolor: "#d6d0f7", tickfont: { size: 11, color: "#6c6993" }, title: { font: { size: 12, color: "#6c6993" } }, automargin: true };
  return {
    autosize: true,
    margin: { l: 52, r: 16, t: 28, b: 44 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { family: "ui-sans-serif, Segoe UI, Inter, system-ui, sans-serif", color: "#1f1b3d", size: 12 },
    colorway: PALETTE,
    hoverlabel: { bgcolor: "#2f2a63", font: { color: "#fff", size: 12 }, bordercolor: "#2f2a63" },
    legend: { orientation: "h", y: -0.22, font: { size: 11 } },
    ...extra,
    xaxis: { ...axis, ...(extra.xaxis ?? {}) },
    yaxis: { ...axis, ...(extra.yaxis ?? {}) },
  };
}

export function PlotChart({ data, layout, height = 320, onPointClick, title, testId }: {
  data: PlotData[]; layout?: Record<string, any>; height?: number; onPointClick?: (point: Record<string, any>) => void; title?: string; testId?: string;
}) {
  const lay = useMemo(() => baseLayout({ height, ...(layout ?? {}) }), [layout, height]);
  const config = useMemo(() => ({ displaylogo: false, responsive: true, modeBarButtonsToRemove: ["lasso2d", "select2d", "autoScale2d"] }), []);
  return (
    <figure className="w-full" data-testid={testId ?? "plot"} data-traces={data.length} aria-label={title}>
      {title && <figcaption className="mb-1 text-sm font-semibold text-ink">{title}</figcaption>}
      <Plot data={data} layout={lay} config={config} useResizeHandler style={{ width: "100%", height }}
        onClick={onPointClick ? (e: any) => e?.points?.[0] && onPointClick(e.points[0]) : undefined} />
    </figure>
  );
}
