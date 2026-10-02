import { z } from "zod";

export class APIError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly requestId?: string,
  ) {
    super(message);
  }
}

export async function api<T>(
  path: string,
  token: string | undefined,
  init?: RequestInit,
  schema?: z.ZodType<T>,
  signal?: AbortSignal,
): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (token) headers.set("Authorization", `Bearer ${token}`);

  const response = await fetch(`/api/v1${path}`, { ...init, signal, headers });
  if (response.status === 401) {
    window.dispatchEvent(new Event("tuba:unauthorized"));
  }
  if (!response.ok) {
    const body = await response.text();
    let message = body || response.statusText;
    let requestId: string | undefined;
    try {
      const parsed = JSON.parse(body) as { message?: string; request_id?: string };
      message = parsed.message || message;
      requestId = parsed.request_id;
    } catch {
      // The API also uses plain-text errors for protocol-level failures.
    }
    throw new APIError(response.status, message, requestId);
  }
  if (response.status === 204) return undefined as T;
  const value = (await response.json()) as T;
  return schema ? schema.parse(value) : value;
}

const entitySchema = z.object({
  id: z.string(),
  type: z.string(),
});

export const anomalySummarySchema = z.object({
  id: z.string(),
  type: z.string(),
  severity: z.enum(["low", "medium", "high", "critical"]),
  status: z.enum(["open", "investigating", "closed", "false_positive"]),
  timestamp: z.string(),
  score: z.number().optional().default(0),
  entity: entitySchema,
  rule_id: z.string().optional().default(""),
  rule_version: z.string().optional().default(""),
  summary: z.string().optional().default(""),
  evidence_count: z.number().int().nonnegative().optional().default(0),
});

export const anomalyDetailSchema = anomalySummarySchema.extend({
  reason_codes: z.array(z.string()).optional().default([]),
  evidence_event_ids: z.array(z.string()).optional().default([]),
});

const evidenceEventSchema = z.object({
  id: z.string(),
  timestamp: z.string(),
  event: z.record(z.string(), z.unknown()),
  user: z.record(z.string(), z.unknown()),
  ueba: z.record(z.string(), z.unknown()),
});

export const evidencePageSchema = z.object({
  items: z.array(evidenceEventSchema),
  total: z.number().int().nonnegative(),
  truncated: z.boolean(),
});

export const anomalyPageSchema = z.object({
  items: z.array(anomalySummarySchema),
  next_cursor: z.string().nullable().optional().default(""),
  total: z.number().int().nonnegative().optional().default(0),
});

export const caseSchema = z.object({
  id: z.string(),
  title: z.string(),
  description: z.string(),
  status: z.enum(["open", "in_progress", "closed"]),
  severity: z.enum(["low", "medium", "high", "critical"]),
  assignee: z.string().optional().default(""),
  verdict: z
    .enum(["true_positive", "benign_positive", "false_positive", "inconclusive", ""])
    .optional(),
  verdict_reason: z.string().optional().default(""),
  version: z.number().int().positive(),
  created_at: z.string(),
  updated_at: z.string(),
  closed_at: z.string().nullable().optional(),
  anomaly_ids: z.array(z.string()).optional().default([]),
});

export const casesSchema = z.object({
  items: z.array(caseSchema),
  next_cursor: z.string().nullable().optional().default(""),
  total: z.number().int().nonnegative().optional().default(0),
});

export const caseActivitySchema = z.object({
  id: z.number(),
  action: z.string(),
  actor: z.string().optional().default(""),
  request_id: z.string(),
  occurred_at: z.string(),
  before_state: z.record(z.string(), z.unknown()).nullable().optional(),
  after_state: z.record(z.string(), z.unknown()).nullable().optional(),
  metadata: z.record(z.string(), z.unknown()),
});

export const caseActivityPageSchema = z.object({
  items: z.array(caseActivitySchema),
  next_cursor: z.string().nullable().optional().default(""),
});

export const memberSchema = z.object({
  subject: z.string(),
  email: z.string().optional().default(""),
  display_name: z.string().optional().default(""),
  role: z.enum(["viewer", "analyst", "tenant_admin"]),
  revoked_at: z.string().optional(),
});

export const membersSchema = z.object({
  items: z.array(memberSchema),
});

export const principalSchema = z.object({
  subject: z.string(),
  organization_id: z.string(),
  namespace: z.string(),
  roles: z.array(z.string()),
  permissions: z.array(z.string()),
});

export const overviewSchema = z.object({
  open_anomalies: z.number().int().nonnegative(),
  high_risk: z.number().int().nonnegative(),
  latest_anomaly: z.string().nullable().optional(),
  generated_at: z.string(),
  cases: z.object({
    active_cases: z.number().int().nonnegative(),
    unassigned_cases: z.number().int().nonnegative(),
    critical_cases: z.number().int().nonnegative(),
    closed_today: z.number().int().nonnegative(),
  }),
});

export const operationsStatusSchema = z.object({
  status: z.enum(["ok", "degraded", "unavailable"]),
  checked_at: z.string(),
  uptime_seconds: z.number().int().nonnegative(),
  dependencies: z.array(
    z.object({
      name: z.string(),
      status: z.enum(["ok", "degraded", "unavailable", "unknown"]),
      detail: z.string(),
      latency_ms: z.number().int().nonnegative().optional(),
    }),
  ),
  analysis: z.object({
    status: z.enum(["ok", "degraded", "unknown"]),
    latest_at: z.string().nullable().optional(),
    age_seconds: z.number().int().nonnegative().nullable().optional(),
    runtime: z
      .object({
        run: z
          .object({
            run_id: z.string(),
            worker_instance: z.string(),
            status: z.enum(["running", "stopped", "failed"]),
            registry_version: z.string(),
            processed_events: z.number().int().nonnegative(),
            emitted_results: z.number().int().nonnegative(),
            started_at: z.string(),
            last_heartbeat_at: z.string(),
            stopped_at: z.string().nullable().optional(),
          })
          .optional(),
        checkpoints: z.array(
          z.object({
            topic: z.string(),
            partition: z.number().int().nonnegative(),
            offset: z.number().int().nonnegative(),
            watermark: z.string().nullable().optional(),
            updated_at: z.string(),
          }),
        ),
      })
      .optional(),
  }),
  kafka_configured: z.boolean(),
});

export const sourceSchema = z.object({
  id: z.string(),
  organization_id: z.string(),
  namespace: z.string(),
  vendor_name: z.string(),
  vendor_product: z.string(),
  vendor_dataset: z.string(),
  source_epoch: z.string(),
  release_id: z.string().optional().default(""),
  state: z.string(),
  enabled: z.boolean(),
  rate_limit: z.number().int().positive(),
  source_context_id: z.string().optional().default(""),
  created_at: z.string(),
  updated_at: z.string(),
});

export const sourcesSchema = z.object({
  items: z.array(sourceSchema),
});

export const collectorSourceStatusSchema = z.object({
  source_id: z.string(),
  state: z.string(),
  events_read: z.number().int().nonnegative(),
  events_sent: z.number().int().nonnegative(),
  events_drop: z.number().int().nonnegative(),
  last_error: z.string().optional().default(""),
});

export const collectorComponentStatusSchema = z.object({
  component: z.string(),
  version: z.string(),
  phase: z.string().optional().default(""),
  state: z.string(),
  restarts: z.number().int().nonnegative(),
  last_error: z.string().optional().default(""),
});

export const collectorHeartbeatSchema = z.object({
  version: z.string().optional().default(""),
  config_version: z.number().int().nonnegative().optional().default(0),
  state: z.string().optional().default(""),
  queue_depth: z.number().int().nonnegative().optional().default(0),
  oldest_queued_at: z.string().nullable().optional(),
  diagnostic: z.string().optional().default(""),
  sources: z.array(collectorSourceStatusSchema).optional().default([]),
  components: z.array(collectorComponentStatusSchema).optional().default([]),
});

export const collectorSummarySchema = z.object({
  id: z.string(),
  organization_id: z.string(),
  namespace: z.string(),
  hostname: z.string(),
  os: z.string(),
  architecture: z.string(),
  installed_version: z.string(),
  desired_version: z.string().optional().default(""),
  state: z.string(),
  config_version: z.number().int().nonnegative(),
  desired_config_version: z.number().int().nonnegative(),
  last_heartbeat_at: z.string().nullable().optional(),
  online: z.boolean(),
  heartbeat: collectorHeartbeatSchema.nullable().optional(),
});

export const collectorsSchema = z.object({
  items: z.array(collectorSummarySchema),
});

export const releaseAssetSchema = z.object({
  kind: z.string(),
  asset_id: z.string(),
  version: z.string().optional().default(""),
  path: z.string().optional().default(""),
  sha256: z.string().optional().default(""),
  dependencies: z.array(z.string()).optional().default([]),
});

export const releaseManifestSchema = z.object({
  schema_version: z.string().optional().default(""),
  release_id: z.string().optional().default(""),
  version: z.string().optional().default(""),
  assets: z.array(releaseAssetSchema).optional().default([]),
  compatibility: z.record(z.string(), z.string()).optional().default({}),
});

export const releaseSchema = z.object({
  id: z.string(),
  version: z.string(),
  manifest: releaseManifestSchema,
  sha256: z.string(),
  state: z.string(),
  created_at: z.string(),
  activated_at: z.string().nullable().optional(),
});

export const releasesSchema = z.object({
  items: z.array(releaseSchema),
});

export const releaseAuditEntrySchema = z.object({
  id: z.number(),
  action: z.string(),
  actor_subject: z.string().optional().default(""),
  request_id: z.string().optional().default(""),
  occurred_at: z.string(),
  before_state: z.record(z.string(), z.unknown()).nullable().optional(),
  after_state: z.record(z.string(), z.unknown()).nullable().optional(),
  metadata: z.record(z.string(), z.unknown()).optional().default({}),
});

export const releaseAuditSchema = z.object({
  items: z.array(releaseAuditEntrySchema),
});

export const catalogFieldSchema = z.object({
  name: z.string(),
  type: z.string(),
  sensitivity: z.string(),
  searchable: z.boolean(),
  aggregable: z.boolean(),
});

export const catalogDatasetSchema = z.object({
  name: z.string(),
  kind: z.string(),
  active_generation: z.string(),
  index_pattern: z.string(),
  quality_statuses: z.array(z.string()).optional().default([]),
  fields: z.array(catalogFieldSchema).optional().default([]),
});

export const catalogSchema = z.object({
  catalog_version: z.number().int().nonnegative(),
  active_generation: z.string(),
  max_time_range_day: z.number().int().positive(),
  datasets: z.array(catalogDatasetSchema),
});

export const queryResultSchema = z.object({
  mode: z.string(),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  next_cursor: z.string().optional().default(""),
  total: z.number().int().nonnegative().optional().default(0),
  aggregations: z.record(z.string(), z.unknown()).optional(),
});

export type QueryResult = z.infer<typeof queryResultSchema>;

export type QueryOptions = { to?: string; limit?: number; cursor?: string };

// The request schema is a closed set server-side (DisallowUnknownFields):
// only query/from/to/limit/cursor may be sent.
export function buildQueryBody(
  query: string,
  from: string | undefined,
  options?: QueryOptions,
): Record<string, string | number> {
  const body: Record<string, string | number> = { query };
  if (from) body.from = from;
  if (options?.to) body.to = options.to;
  if (options?.limit) body.limit = options.limit;
  if (options?.cursor) body.cursor = options.cursor;
  return body;
}

export function runQuery(
  token: string | undefined,
  query: string,
  from: string | undefined,
  signal?: AbortSignal,
  options?: QueryOptions,
): Promise<QueryResult> {
  return api(
    "/query",
    token,
    { method: "POST", body: JSON.stringify(buildQueryBody(query, from, options)) },
    queryResultSchema,
    signal,
  );
}

export type StatsBucket = { key: Record<string, string>; count: number };

export function statsBuckets(result: QueryResult): StatsBucket[] {
  const agg = result.aggregations?.["buckets"] as
    | { buckets?: Array<{ key?: Record<string, unknown>; doc_count?: number }> }
    | undefined;
  return (agg?.buckets ?? []).map((bucket) => {
    const key: Record<string, string> = {};
    for (const [field, value] of Object.entries(bucket.key ?? {})) {
      key[field] = String(value);
    }
    return { key, count: bucket.doc_count ?? 0 };
  });
}

export function statsTotal(result: QueryResult): number {
  const agg = result.aggregations?.["count"] as { value?: number } | undefined;
  return agg?.value ?? 0;
}

// A single-value metric aggregation (count/sum/min/max/avg [as alias]).
// Date metrics carry value_as_string alongside the epoch-millis value.
export type StatsMetric = { value: number | null; valueAsString?: string };

export function statsMetric(result: QueryResult, name: string): StatsMetric | undefined {
  const agg = result.aggregations?.[name] as
    | { value?: number | null; value_as_string?: string }
    | undefined;
  if (!agg || typeof agg !== "object") return undefined;
  return { value: agg.value ?? null, valueAsString: agg.value_as_string };
}

// Provenance references carried on UIM events (catalog-declared fields). Raw
// dataset rows carry ueba.provenance.source_context_id instead.
export type EventProvenance = {
  eventId: string;
  rawEventId: string;
  releaseId: string;
  domain: string;
  generation: string;
  sourceContextId: string;
  schemaVersion: string;
  qualityStatus: string;
};

function nestedRecord(item: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = item[key];
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function eventProvenance(item: Record<string, unknown>): EventProvenance {
  const event = nestedRecord(item, "event");
  const ueba = nestedRecord(item, "ueba");
  const route = nestedRecord(ueba, "route");
  const provenance = nestedRecord(ueba, "provenance");
  const quality = nestedRecord(ueba, "quality");
  const schema = nestedRecord(ueba, "schema");
  const text = (value: unknown) => (typeof value === "string" ? value : "");
  return {
    eventId: text(event["id"] ?? item["id"]),
    rawEventId: text(provenance["raw_event_id"]),
    releaseId: text(provenance["release_id"]),
    domain: text(route["domain"] ?? event["dataset"]),
    generation: text(route["generation"]),
    sourceContextId: text(provenance["source_context_id"]),
    schemaVersion: text(schema["version"]),
    qualityStatus: text(quality["status"]),
  };
}

export const exportJobSchema = z.object({
  id: z.string(),
  job_id: z.string(),
  dataset: z.string(),
  format: z.string(),
  query: z.string(),
  from: z.string(),
  to: z.string(),
  row_limit: z.number().int().positive(),
  byte_limit: z.number().int().positive(),
  include_sensitive: z.boolean(),
  include_raw: z.boolean(),
  state: z.string(),
  retention_from: z.string().nullable().optional(),
  limit_reached: z.string().optional().default(""),
  file_sha256: z.string().optional().default(""),
  row_count: z.number().int().nonnegative().nullable().optional(),
  byte_count: z.number().int().nonnegative().nullable().optional(),
  expires_at: z.string().nullable().optional(),
  downloaded_at: z.string().nullable().optional(),
  completed_at: z.string().nullable().optional(),
  error: z.string().optional().default(""),
  created_at: z.string(),
});

export const exportsSchema = z.object({
  items: z.array(exportJobSchema),
  next_cursor: z.string().optional().default(""),
});

export const analysisFeedbackSchema = z.object({
  id: z.number(),
  feedback_type: z.enum(["true_positive", "false_positive", "false_negative", "inconclusive"]),
  anomaly_id: z.string().optional().default(""),
  rule_id: z.string().optional().default(""),
  entity_id: z.string().optional().default(""),
  event_ids: z.array(z.string()).default([]),
  reason: z.string(),
  created_at: z.string(),
});

export type AnomalySummary = z.infer<typeof anomalySummarySchema>;
export type AnomalyDetail = z.infer<typeof anomalyDetailSchema>;
export type EvidenceEvent = z.infer<typeof evidenceEventSchema>;
export type Case = z.infer<typeof caseSchema>;
export type CaseActivity = z.infer<typeof caseActivitySchema>;
export type Member = z.infer<typeof memberSchema>;
export type Principal = z.infer<typeof principalSchema>;
export type SourceInstance = z.infer<typeof sourceSchema>;
export type CollectorSummary = z.infer<typeof collectorSummarySchema>;
export type Overview = z.infer<typeof overviewSchema>;
export type OperationsStatus = z.infer<typeof operationsStatusSchema>;
export type AnalysisFeedback = z.infer<typeof analysisFeedbackSchema>;
export type Release = z.infer<typeof releaseSchema>;
export type ReleaseAuditEntry = z.infer<typeof releaseAuditEntrySchema>;
export type CatalogDataset = z.infer<typeof catalogDatasetSchema>;
export type ExportJob = z.infer<typeof exportJobSchema>;

export type EntityAggregate = {
  id: string;
  type: string;
  anomalyCount: number;
  openCount: number;
  latest: string;
  highestSeverity: AnomalySummary["severity"];
};

const severityRank: Record<AnomalySummary["severity"], number> = {
  low: 0,
  medium: 1,
  high: 2,
  critical: 3,
};

export function groupEntities(items: AnomalySummary[]): EntityAggregate[] {
  const byId = new Map<string, EntityAggregate>();
  for (const item of items) {
    if (!item.entity.id) continue;
    const current = byId.get(item.entity.id);
    if (!current) {
      byId.set(item.entity.id, {
        id: item.entity.id,
        type: item.entity.type,
        anomalyCount: 1,
        openCount: item.status === "open" ? 1 : 0,
        latest: item.timestamp,
        highestSeverity: item.severity,
      });
      continue;
    }
    current.anomalyCount += 1;
    if (item.status === "open") current.openCount += 1;
    if (item.timestamp > current.latest) current.latest = item.timestamp;
    if (severityRank[item.severity] > severityRank[current.highestSeverity]) {
      current.highestSeverity = item.severity;
    }
  }
  return [...byId.values()].sort((a, b) => b.latest.localeCompare(a.latest));
}

export function anomalyWindow(hours: number): { from: string; to: string } {
  const to = new Date();
  const from = new Date(to.getTime() - hours * 3600 * 1000);
  return { from: from.toISOString(), to: to.toISOString() };
}

export function queryString(values: Record<string, string | number | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined && value !== "") query.set(key, String(value));
  }
  const encoded = query.toString();
  return encoded ? `?${encoded}` : "";
}

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "未知";
  if (seconds < 60) return `${Math.round(seconds)} 秒`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} 分钟`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)} 小时`;
  return `${Math.round(seconds / 86400)} 天`;
}
