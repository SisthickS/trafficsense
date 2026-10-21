"use client";

import { useMemo } from "react";
import { useApi } from "@/lib/hooks";
import { qs } from "@/lib/api";
import { buildFigure, type ChartSpec, type Row } from "@/lib/charts";
import { PlotChart } from "@/components/PlotChart";
import { Card, ErrorState, Skeleton } from "@/components/ui";
import type { TableData } from "@/lib/types";

export const TABLE_CAP = 500;

/** One Plotly chart drawn from an analysis table that the API serves from MongoDB. */
export function ChartCard({ qid, spec, highlight = null }: { qid: string; spec: ChartSpec; highlight?: number | null }) {
  const res = useApi<TableData>(`/api/v1/analytics/questions/${qid}/tables/${spec.table}${qs({ limit: TABLE_CAP })}`);
  const fig = useMemo(() => (res.data ? buildFigure(spec, res.data.rows as Row[], { highlightSensor: highlight }) : null), [res.data, spec, highlight]);
  return (
    <Card className={spec.kind === "scatter" && spec.height ? "md:col-span-2" : ""}>
      {res.error && !res.data ? <ErrorState error={res.error} retry={() => res.mutate()} /> : !fig ? <Skeleton h={spec.height ?? 300} /> : (
        <>
          <PlotChart data={fig.data} layout={fig.layout} title={spec.title} height={spec.height ?? 300} testId={`chart-${qid}-${spec.id}`} />
          <p className="mt-1 text-[11px] text-slate-400">
            source: analysis_results · {qid}/{spec.table} · {res.data!.rows.length.toLocaleString("en-US")} rows
            {res.data!.n_rows > res.data!.rows.length && ` (first ${TABLE_CAP} of ${res.data!.n_rows.toLocaleString("en-US")})`}
            {spec.note && <span className="ml-2 text-muted">{spec.note}</span>}
          </p>
        </>
      )}
    </Card>
  );
}
