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
  hold: z.boolean().optional().default(false),
  hold_reason: z.string().optional().default(""),
  hold_at: z.string().nullable().optional(),
});

export const casesSchema = z.object({
  items: z.array(caseSchema),
  next_cursor: z.string().nullable().optional().default(""),
  total: z.number().int().nonnegative().optional().default(0),
});

export const caseLinkSchema = z.object({
  link_type: z.enum(["entity", "risk_contribution", "evidence"]),
  ref_kind: z.string(),
  target_id: z.string(),
  linked_by: z.string().optional().default(""),
  linked_at: z.string(),
});

export const caseLinksSchema = z.object({
  items: z.array(caseLinkSchema).optional().default([]),
});

// The frozen snapshot body is an opaque jsonb document; keep it as a loose
// record so any frozen shape renders without schema drift.
export const caseSnapshotSchema = z.object({
  id: z.string(),
  case_id: z.string(),
  label: z.string(),
  snapshot: z.record(z.string(), z.unknown()).optional().default({}),
  created_by: z.string().optional().default(""),
  created_at: z.string(),
});

export const caseSnapshotsSchema = z.object({
  items: z.array(caseSnapshotSchema).optional().default([]),
});

// Entity profile endpoints (W03 backend): PG entity registry reads plus the
// ES entity_risk state projection read-back.
export const entitySummarySchema = z.object({
  entity_id: z.string(),
  entity_type: z.enum(["account", "device"]),
  authority: z.string(),
  canonical_key: z.string(),
  identity_strength: z.enum(["strong", "weak"]),
  revision: z.number().int().positive(),
  valid_from: z.string(),
  valid_to: z.string().nullable().optional(),
});

export const entityPageSchema = z.object({
  items: z.array(entitySummarySchema).optional().default([]),
  next_cursor: z.string().nullable().optional().default(""),
});

export const entityAttributionSchema = z.object({
  attribution_id: z.string(),
  event_id: z.string(),
  role: z.string(),
  state: z.enum(["resolved", "unresolved", "ambiguous"]),
  rule_version: z.string().optional().default(""),
  reason: z.string().optional().default(""),
  event_time: z.string(),
  evidence: z.record(z.string(), z.unknown()).optional(),
});

export const entityAttributionPageSchema = z.object({
  items: z.array(entityAttributionSchema).optional().default([]),
  next_cursor: z.string().nullable().optional().default(""),
});

export const entityDetailSchema = entitySummarySchema.extend({
  document: z.record(z.string(), z.unknown()).optional().default({}),
  recent_attributions: z.array(entityAttributionSchema).optional().default([]),
});

export const entityRelationSchema = z.object({
  relation_id: z.string(),
  from_entity_id: z.string(),
  relation_type: z.string(),
  to_entity_id: z.string(),
  event_id: z.string(),
  rule_version: z.string().optional().default(""),
  valid_from: z.string(),
  valid_to: z.string().nullable().optional(),
  confidence: z.number().min(0).max(1),
  evidence: z.record(z.string(), z.unknown()).optional(),
});

export const entityRelationsSchema = z.object({
  items: z.array(entityRelationSchema).optional().default([]),
});

export const entityFeatureSampleSchema = z.object({
  feature_id: z.string(),
  feature_version: z.string(),
  generation: z.string(),
  window_start: z.string(),
  window_end: z.string(),
  revision: z.number().int().positive(),
  quality: z.enum(["qualified", "partial"]),
  values: z.record(z.string(), z.unknown()).optional().default({}),
});

export const entityFeaturesSchema = z.object({
  items: z.array(entityFeatureSampleSchema).optional().default([]),
});

export const entityBaselineModelSchema = z.object({
  model_id: z.string(),
  model_version: z.string(),
  feature_id: z.string(),
  feature_version: z.string(),
  generation: z.string(),
  status: z.enum(["cold_start", "training", "ready", "retired"]),
  sample_count: z.number().int().nonnegative(),
  complete_days: z.number().int().nonnegative(),
  trained_at: z.string().nullable().optional(),
  training_cutoff: z.string().nullable().optional(),
  sample_range: z.record(z.string(), z.unknown()).optional().default({}),
  metrics: z.record(z.string(), z.unknown()).optional().default({}),
  created_at: z.string(),
  covers_entity: z.boolean().optional().default(false),
});

export const entityBaselineSchema = z.object({
  items: z.array(entityBaselineModelSchema).optional().default([]),
});

// The risk projection document is the R02 pipeline output (risk_score,
// contributions, decay, compute_version, updated_at); projection is null when
// the entity has no finding contributions yet.
export const entityRiskSchema = z.object({
  entity_id: z.string(),
  projection: z.record(z.string(), z.unknown()).nullable(),
  revision: z.number().int().positive().optional(),
  operation: z.enum(["upsert", "retracted"]).optional(),
});

export type EntitySummary = z.infer<typeof entitySummarySchema>;
export type EntityDetail = z.infer<typeof entityDetailSchema>;
export type EntityAttribution = z.infer<typeof entityAttributionSchema>;
export type EntityRelation = z.infer<typeof entityRelationSchema>;
export type EntityFeatureSample = z.infer<typeof entityFeatureSampleSchema>;
export type EntityBaselineModel = z.infer<typeof entityBaselineModelSchema>;
export type EntityRisk = z.infer<typeof entityRiskSchema>;

export function listEntities(
  token: string | undefined,
  params: { query?: string; type?: string; cursor?: string; limit?: number },
  signal?: AbortSignal,
) {
  return api(
    `/entities${queryString({
      query: params.query,
      type: params.type,
      cursor: params.cursor,
      limit: params.limit,
    })}`,
    token,
    undefined,
    entityPageSchema,
    signal,
  );
}

export function getEntity(token: string | undefined, id: string, signal?: AbortSignal) {
  return api(`/entities/${encodeURIComponent(id)}`, token, undefined, entityDetailSchema, signal);
}

export function listEntityAttributions(
  token: string | undefined,
  id: string,
  params: { cursor?: string; limit?: number },
  signal?: AbortSignal,
) {
  return api(
    `/entities/${encodeURIComponent(id)}/attributions${queryString({ cursor: params.cursor, limit: params.limit })}`,
    token,
    undefined,
    entityAttributionPageSchema,
    signal,
  );
}

export function listEntityRelations(token: string | undefined, id: string, history: boolean, signal?: AbortSignal) {
  return api(
    `/entities/${encodeURIComponent(id)}/relations${queryString({ history: history ? "true" : "false" })}`,
    token,
    undefined,
    entityRelationsSchema,
    signal,
  );
}

export function listEntityFeatures(token: string | undefined, id: string, limit?: number, signal?: AbortSignal) {
  return api(
    `/entities/${encodeURIComponent(id)}/features${queryString({ limit })}`,
    token,
    undefined,
    entityFeaturesSchema,
    signal,
  );
}

export function getEntityBaseline(token: string | undefined, id: string, signal?: AbortSignal) {
  return api(`/entities/${encodeURIComponent(id)}/baseline`, token, undefined, entityBaselineSchema, signal);
}

export function getEntityRisk(token: string | undefined, id: string, signal?: AbortSignal) {
  return api(`/entities/${encodeURIComponent(id)}/risk`, token, undefined, entityRiskSchema, signal);
}

export const auditEventSchema = z.object({
  id: z.number(),
  action: z.string(),
  resource_type: z.string().optional().default(""),
  resource_id: z.string().optional().default(""),
  request_id: z.string().optional().default(""),
  occurred_at: z.string(),
  metadata: z.record(z.string(), z.unknown()).nullable().optional(),
});

export const auditPageSchema = z.object({
  items: z.array(auditEventSchema).optional().default([]),
  next_cursor: z.string().nullable().optional().default(""),
});

export const feedbackEvaluationRowSchema = z.object({
  feedback_id: z.number(),
  feedback_type: z.string(),
  anomaly_id: z.string().optional().default(""),
  rule_id: z.string().optional().default(""),
  entity_id: z.string().optional().default(""),
  event_ids: z.array(z.string()).optional().default([]),
  reason: z.string().optional().default(""),
  actor: z.string().optional().default(""),
  source_case_id: z.string().nullable().optional(),
  target_case_id: z.string().nullable().optional(),
  created_at: z.string(),
  finding_label: z.string().optional().default(""),
  labeled_at: z.string().nullable().optional(),
  case_verdict: z.string().optional().default(""),
});

export const feedbackEvaluationSchema = z.object({
  rows: z.array(feedbackEvaluationRowSchema).optional().default([]),
});

export const feedbackRuleMetricsSchema = z.object({
  rule_id: z.string(),
  true_positive: z.number().int().nonnegative(),
  false_positive: z.number().int().nonnegative(),
  false_negative: z.number().int().nonnegative(),
  inconclusive: z.number().int().nonnegative(),
  precision: z.number().min(0).max(1),
});

export const feedbackMetricsSchema = z.object({
  rules: z.array(feedbackRuleMetricsSchema).optional().default([]),
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
export type CaseLink = z.infer<typeof caseLinkSchema>;
export type CaseSnapshot = z.infer<typeof caseSnapshotSchema>;
export type AuditEvent = z.infer<typeof auditEventSchema>;
export type FeedbackEvaluationRow = z.infer<typeof feedbackEvaluationRowSchema>;
export type FeedbackRuleMetrics = z.infer<typeof feedbackRuleMetricsSchema>;
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
