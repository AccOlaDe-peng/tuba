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
