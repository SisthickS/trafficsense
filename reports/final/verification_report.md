# TrafficSense — Final Verification Report (Phase 10)

Date of run: 2026-10-21 · Platform: Windows 11 host, WSL2 Ubuntu, Python 3.11, Node 22, Next.js 16.4, React 19, Spark 3.5.9, MongoDB 7.
Reproduce everything with `bash scripts/run_phase10.sh` (logs in `reports/final/logs/`).

## Verdict

The whole platform works end to end: data prepared by Spark → MLlib model → Kafka/Structured Streaming → MongoDB → FastAPI → Next.js dashboard.
All automated layers pass and every dashboard page was exercised against live data. Two documented limitations (no Chromium-based test runner, no `.env.example`) are listed at the end.

## 1. Automated test results

| Layer | Command | Result |
|---|---|---|
| Full Python suite (all phases: preprocessing, features, Spark benchmarks, analytics, modelling, MLlib, streaming, Mongo, API, integration) | `pytest -q tests` | **130 passed** (272 s) |
| Integration subset (contract + end-to-end + API + Mongo) | `pytest -q tests/test_frontend_contract.py tests/test_integration_e2e.py tests/test_api.py tests/test_mongo.py` | **67 passed** (65 s) |
| Frontend ↔ API contract | `tests/test_frontend_contract.py` | 3 passed: every `/api/v1/…` path used by the frontend source exists in the FastAPI OpenAPI schema; all 10 router groups are consumed |
| End-to-end (own API :8011 + own production dashboard :3111, real MongoDB) | `tests/test_integration_e2e.py` | 6 passed (see 2) |
| Frontend integration (vitest + jsdom, **real API**, only Plotly stubbed) | `cd frontend && npm test` | **29 passed** in 3 files |
| TypeScript | `npm run typecheck` | 0 errors |
| Production build | `npm run build` | success; 10 routes (8 pages, `/_not-found`, `/backend/[...path]` proxy) |

Test counts before Phase 10 were 120; Phase 10 adds 10 Python tests (3 contract, 6 end-to-end, 1 API test for the new endpoints) and 29 frontend tests.

## 2. What the end-to-end tests prove (`tests/test_integration_e2e.py`)

| Test | Evidence |
|---|---|
| Every page renders with the persistent navigation | 8 routes return 200, contain the nav links to all 8 pages and the project-scope strip; unknown route → 404 |
| Proxy serves the same live data as the API | `/status` counts, Q5 document, hotspots, model evaluations and performance overview are identical through the proxy and directly |
| Every question is served | Q1–Q18: findings + tables present, first table returns rows |
| Live traffic data | latest predictions are present with probabilities in [0, 1] |
| CRUD through the proxy persists in MongoDB | POST → document read with an **independent pymongo client**; duplicate → 409; PATCH with `If-Match` → version 2; stale `If-Match` → 412 and no change; invalid body (latitude 999) → 422 and nothing stored; DELETE → 204, document gone, GET → 404 |
| Persistence across an API crash | setting written via the dashboard proxy → `kill -9` of the API → proxy answers **502 `api_unreachable`** (envelope) while pages still render → API restarted → value still stored; setting restored afterwards |

## 3. Frontend integration tests (`frontend/tests/`, run against the live API)

| File | Tests | What it covers |
|---|---|---|
| `lib.test.ts` | 6 | query-string/number helpers; readiness + status; error envelope → `ApiError` (404, 422 with field details); create → PATCH(If-Match) → stale write 412 → delete; setting put/validate/restore |
| `charts.test.ts` | 20 | all 18 questions have chart specs; for **every chart spec of every question** the real table is fetched from the API and `buildFigure` must return non-empty traces with points; Q1 findings/implications/limitations served |
| `data-status.test.tsx` | 3 | the real `SensorsTab` driven with a user-event: add sensor (verified in API), edit (version 2), inline status change, client-side validation, delete (404 afterwards); settings tab lists live settings; Status page shows the 8 MongoDB collections, API readiness and run history |

## 4. Browser verification (editor's integrated browser, production build, live data)

| Check | Result |
|---|---|
| Landing page | hero, project-scope strip (325 sensors, 16.9 M readings, 01 Jan–30 Jun 2017, 18 questions), live risk card (2 sensors ≥ 0.686 at slot 2017-06-05 17:50), best model card (F1 0.930, recall 98.7 %, 59 % saving), summary cards, 8-stage pipeline workflow with run counts from MongoDB, insights from Q1/Q5/Q10/Q16 |
| Traffic Analytics Q1–Q18 | all 18 questions load: 2–4 Plotly charts each, 8–12 findings/recommendation items, no error panel (Q1 4, Q2 2, Q3 3, … Q13 4, Q18 4 charts) |
| Other pages (Hotspots, Live, Models, Performance, Data, Status; landing has no charts) | no error panels, no failed network request, no JS page error: Hotspots (2 charts, 12-row table), Live (2 charts, 19-row table), Models (9 charts, 5 model rows), Performance (10 charts: 7 benchmark groups + 3 stream charts), Data (10-row table), Status (2 charts, 27 table rows) |
| Data Management CRUD in the browser | add sensor 9900001 → API shows v1 with tags → edit name → v2 → inline status "maintenance" → v3 → page reload: row still present → delete → API 404 |
| Live page | Playback mode shows slot slider over the 71 stored slots (17:50 → 2 alerts; 15:20 → 91 alerts); lowering the threshold to 0.55 and saving stored `alert_threshold = 0.55` (v1) in MongoDB; restored to default afterwards |
| Performance page data | every benchmark chart has data (scan 5 cases, size 5, shuffle 5, cache 5+5, joins 3, jobs 18, model training 11) |

### Defects found by this verification and fixed

1. **Links lost their colour** — an unlayered `a { color: inherit }` overrode Tailwind v4 layered utilities, so the white "Explore the analysis" hero button had white text. Moved the rule into `@layer base`.
2. **Hard-coded claim on the Models page** — "Every model is cheaper than persistence in 100 % of 1,000 bootstrap resamples" was static text. Data shows the *historical-profile baseline is more expensive* than persistence (cost 288,523 vs 116,706; cheaper in 0 % of resamples), so the sentence is now generated from the stored bootstrap shares of the trained models.
3. **Test hygiene** — `/status/database` includes a live `ping_ms`, so the proxy-parity test compares its stable fields only; the settings test now restores state in a `finally` block.

## 5. Performance (see `performance_summary.md`)

API p50 5–35 ms direct (+6–12 ms through the proxy); page shells 5–7 ms; JS bundle 709 KB gzip (Plotly chunk lazily loaded); ≈ 70–110 requests/s on one worker; streaming p50 latency 9.9 s vs the 300 s horizon;
Parquet 20.9× faster scans; caching 19× faster reuse.

## 6. Requirements coverage

50 requirements in `traceability_matrix.md`: 49 verified (✔), 1 with a stated limitation (◐, R-35 `.env.example`). Presentation outline: `presentation_outline.md`.

## 7. Limitations and honest notes

- **No Playwright/Chromium suite.** The WSL VM lacks the browser system libraries and sudo, so the automated UI tests run in jsdom with Plotly stubbed; real rendering was checked manually in the editor's browser (a hidden tab there stalls Playwright actionability checks, so some interactions were triggered through DOM events). A CI job with a full browser should be added.
- **`.env.example` was not created** — an organization content policy blocks that file in this workspace; all variables are documented in the README tables. This is requirement R-35.
- **Maps** are Plotly scatter plots of longitude/latitude, not tile maps.
- **MongoDB** runs without authentication, bound to `127.0.0.1` (development). API writes can be protected with `TRAFFICSENSE_API_KEY`; the dashboard proxy adds it server-side.
- **Analysis tables with more than 500 rows** (e.g. Q7 edges, 2,369 rows) show the first 500 (API maximum page size); the chart footers state this.
- The benchmark machine is shared with the editor and Docker; throughput varies between runs (46–189 req/s observed for the same endpoint), so numbers show orders of magnitude.
- The streaming job is not running permanently; the dashboard shows the stored replay (6 h of 2017-06-05). Start `src.streaming.traffic_stream` plus the producer (README Phase 8) to see new slots arrive in *Follow latest* mode.
