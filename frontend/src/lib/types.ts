/** API response types (the subset of the FastAPI schemas the dashboard uses). */

export interface Paged<T> { items: T[]; total: number; limit: number; offset: number }

export interface Sensor {
  sensor_id: number; name: string | null; description: string | null; direction: string | null; lanes: number | null;
  status: "active" | "inactive" | "maintenance" | "decommissioned"; tags: string[]; latitude: number; longitude: number;
  stats: Record<string, number | null>; source: "dataset" | "api"; created_at: string; updated_at: string; version: number;
  hotspot?: { rank: number; class: string; is_hotspot: boolean; persistence: number | null; mean_daily_congested_min: number | null; csbi: number | null; csbi_rank: number | null; tti_peak: number | null } | null;
}

export interface Setting {
  key: string; value: unknown; known: boolean; category: string; description: string | null; type: string; default: unknown;
  minimum: number | null; maximum: number | null; choices: string[] | null; is_default: boolean; version: number | null;
  created_at: string | null; updated_at: string | null; updated_by: string | null;
}

export interface Hotspot {
  sensor_id: number; rank: number; hotspot_class: string; is_hotspot: boolean; persistence: number | null; mean_daily_congested_min: number | null;
  max_daily_congested_min: number | null; workdays: number | null; csbi: number | null; csbi_rank: number | null; tti_peak: number | null;
  latitude: number; longitude: number;
}

export interface Prediction {
  sensor_id: number; slot: number; event_ts: string; target_ts: string; speed_mph: number; probability: number; alert: boolean;
  stored_alert: boolean; model: string | null; is_imputed: boolean | null;
}
export interface LatestPredictions { slot: number | null; event_ts: string | null; threshold: number; total: number; limit: number; offset: number; items: Prediction[] }
export interface SlotSummary { slot: number; event_ts: string; predictions: number; alerts: number; mean_probability: number; max_probability: number }

export interface Question {
  qid: string; number: number; title: string; question: string; metrics: Record<string, unknown>; generated_at: string; findings: number; tables: string[]; figures: string[];
}
export interface QuestionDetail extends Omit<Question, "findings"> {
  findings: string[]; implications: string[]; limitations: string[]; figure_urls: string[]; seconds: number | null; report: string;
}
export interface TableData { qid: string; name: string; columns: string[]; n_rows: number; limit: number; offset: number; rows: Record<string, unknown>[] }

export interface MetricPoint {
  threshold: number; precision: number; recall: number; f1: number; accuracy: number; pr_auc: number; roc_auc: number; onset_recall: number;
  false_alarm_rate: number; miss_rate: number; cost: number; cost_per_1000: number; confusion: { tp: number; fp: number; fn: number; tn: number };
}
export interface Evaluation {
  model: string; label: string; kind: "model" | "baseline"; is_best: boolean; params: Record<string, unknown> | null; operating_threshold: number | null;
  model_path: string | null; test: { f1_optimal: MetricPoint; cost_optimal: MetricPoint };
  validation: { pr_auc: number; roc_auc: number } | null; cost_model: { fn: number; fp: number };
  cost_vs_persistence: { total_cost: number; saving_pct: number; saving_ci_low: number; saving_ci_high: number; share_boot_cheaper: number } | null;
  f1_vs_persistence: { f1: number; f1_ci_low: number; f1_ci_high: number; delta_vs_persistence: number } | null; evaluated_at: string;
}
export interface PredictResult {
  model: string; threshold: number; threshold_source: string; count: number; inference_ms: number; cold_start: boolean;
  predictions: { index: number; sensor_id: number | null; probability: number; alert: boolean; imputed_features: number }[];
}
export interface ModelInfo { available: boolean; loaded: boolean; label?: string; algorithm?: string; default_threshold?: number; stages?: string[]; load_seconds?: number | null; predictions_served?: number; features?: string[] }

export interface PipelineRun {
  _id?: string; kind: string; name: string; status: "succeeded" | "failed" | "running"; finished_at: string; started_at?: string; duration_seconds?: number | null;
  summary?: Record<string, unknown>; artifacts?: string[]; recorded_at?: string;
}
export interface KindStatus { kind: string; runs: number; failed: number; latest_name: string; latest_finished_at: string; latest_status: string }

export interface StreamSummary {
  run: string; query: string; batches: number; batches_with_data: number; input_rows: number; batch_ms_p50: number; batch_ms_p95: number; batch_ms_max: number;
  mean_processed_rows_per_s: number; first_at: string; last_at: string;
}
export interface StreamReport { run: string; queries: Record<string, Record<string, number | string | null>>; end_to_end_latency: Record<string, any>; mongo_counts: Record<string, number> }

export interface StatusOverview {
  version: string; env: string; database: string; counts: Record<string, number>; sensors_by_status: Record<string, number>; newest_prediction_event_ts: string | null;
  write_protection: string; model: { loaded: boolean; enabled: boolean }; pipeline: KindStatus[];
}
export interface DatabaseInfo {
  database: string; data_mb: number; storage_mb: number; index_mb: number; server_version: string; ping_ms: number;
  collections: { name: string; documents: number; indexes: string[]; validator: boolean }[];
}
export interface PerformanceOverview {
  spark_benchmarks: Record<string, any>; analytics_jobs: { qid: string; seconds: number | null; status: string }[];
  ml_training: { summary: { candidates_table?: Record<string, any>[]; best_algorithm?: string } } | null;
  streaming: { summary: StreamSummary[]; reports: StreamReport[] };
}
