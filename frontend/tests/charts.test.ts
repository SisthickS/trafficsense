import { describe, expect, it } from "vitest";
import { request } from "@/lib/api";
import { QUESTION_CHARTS, buildFigure, type Row } from "@/lib/charts";
import type { TableData } from "@/lib/types";

const hasPoints = (trace: Record<string, unknown>) =>
  Object.entries(trace).some(([k, v]) => k !== "marker" && Array.isArray(v) && v.length > 0);

describe("chart specifications vs. the real MongoDB analysis tables", () => {
  it("covers all 18 questions", () => {
    expect(Object.keys(QUESTION_CHARTS).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)))).toEqual(Array.from({ length: 18 }, (_, i) => `q${i + 1}`));
  });

  for (const [qid, specs] of Object.entries(QUESTION_CHARTS)) {
    it(`${qid}: every chart draws non-empty traces from served data`, async () => {
      expect(specs.length).toBeGreaterThan(0);
      for (const spec of specs) {
        const t = await request<TableData>(`/api/v1/analytics/questions/${qid}/tables/${spec.table}?limit=500`);
        expect(t.data.rows.length, `${qid}/${spec.table} has rows`).toBeGreaterThan(0);
        const fig = buildFigure(spec, t.data.rows as Row[]);
        expect(fig.data.length, `${qid}/${spec.id} has traces`).toBeGreaterThan(0);
        expect(fig.data.some(hasPoints), `${qid}/${spec.id} has points`).toBe(true);
      }
    });
  }

  it("q1 findings, implications and limitations are served with the question", async () => {
    const q = await request<{ findings: string[]; implications: string[]; limitations: string[] }>("/api/v1/analytics/questions/q1");
    expect(q.data.findings.length).toBeGreaterThan(0);
    expect(q.data.implications.length).toBeGreaterThan(0);
    expect(q.data.limitations.length).toBeGreaterThan(0);
  });
});
