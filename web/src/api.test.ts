import { describe, expect, it } from "vitest";

import {
  anomalyPageSchema,
  anomalySummarySchema,
  anomalyWindow,
  buildQueryBody,
  caseSchema,
  casesSchema,
  catalogSchema,
  collectorsSchema,
  eventProvenance,
  exportsSchema,
  groupEntities,
  queryResultSchema,
  queryString,
  releasesSchema,
  sourcesSchema,
  statsBuckets,
  statsMetric,
  statsTotal,
} from "./api";

describe("API contracts", () => {
  it("rejects an incomplete case", () => {
    expect(() => caseSchema.parse({ id: "1" })).toThrow();
  });

  it("accepts a paged case response", () => {
    expect(
      casesSchema.parse({
        items: [
          {
            id: "1",
            title: "Case",
            description: "",
            status: "open",
            severity: "high",
            version: 1,
            created_at: "2026-09-24T00:00:00Z",
            updated_at: "2026-09-24T00:00:00Z",
          },
        ],
        next_cursor: "",
      }).items,
    ).toHaveLength(1);
  });

  it("validates normalized anomaly summaries", () => {
    const value = anomalySummarySchema.parse({
      id: "anom-1",
      type: "auth.failure-then-success",
      severity: "high",
      status: "open",
      timestamp: "2026-09-24T00:00:00Z",
      entity: { id: "zhang.wei", type: "account" },
      evidence_count: 6,
    });
    expect(value.entity.id).toBe("zhang.wei");
    expect(value.score).toBe(0);
  });

  it("omits empty filters from query strings", () => {
    expect(queryString({ status: "open", severity: "", limit: 50 })).toBe("?status=open&limit=50");
  });

  it("accepts source list entries with lifecycle state", () => {
    const value = sourcesSchema.parse({
      items: [
        {
          id: "src_1",
          organization_id: "tenant_a",
          namespace: "tenant_a",
          vendor_name: "Microsoft",
          vendor_product: "Windows",
          vendor_dataset: "Security",
          source_epoch: "1",
          state: "active",
          enabled: true,
          rate_limit: 100,
          created_at: "2026-10-01T00:00:00Z",
          updated_at: "2026-10-01T00:00:00Z",
        },
      ],
    });
    expect(value.items[0]!.state).toBe("active");
    expect(value.items[0]!.release_id).toBe("");
  });

  it("keeps management and collection fields separate on collector summaries", () => {
    const value = collectorsSchema.parse({
      items: [
        {
          id: "col_0123456789abcdef0123456789abcdef",
          organization_id: "tenant_a",
          namespace: "tenant_a",
          hostname: "win-169",
          os: "windows",
          architecture: "amd64",
          installed_version: "8.19.0",
          state: "running",
          config_version: 3,
          desired_config_version: 4,
          last_heartbeat_at: "2026-10-09T00:00:00Z",
          online: true,
          heartbeat: {
            state: "running",
            queue_depth: 12,
            sources: [
              { source_id: "src_1", state: "running", events_read: 10, events_sent: 9, events_drop: 1 },
            ],
          },
        },
      ],
    });
    const collector = value.items[0]!;
    expect(collector.state).toBe("running");
    expect(collector.desired_config_version).toBe(4);
    expect(collector.heartbeat?.queue_depth).toBe(12);
    expect(collector.heartbeat?.sources[0]?.events_sent).toBe(9);
  });

  it("accepts collectors that never reported a heartbeat", () => {
    const value = collectorsSchema.parse({
      items: [
        {
          id: "col_0123456789abcdef0123456789abcdef",
          organization_id: "tenant_a",
          namespace: "tenant_a",
          hostname: "new-host",
          os: "linux",
          architecture: "amd64",
          installed_version: "",
          state: "enrolled",
          config_version: 0,
          desired_config_version: 0,
          online: false,
        },
      ],
    });
    expect(value.items[0]!.heartbeat).toBeUndefined();
    expect(value.items[0]!.online).toBe(false);
  });

  it("parses release bundles with manifest assets", () => {
    const value = releasesSchema.parse({
      items: [
        {
          id: "windows-security-1.0.0",
          version: "1.0.0",
          manifest: {
            schema_version: "1.0.0",
            release_id: "windows-security-1.0.0",
            version: "1.0.0",
            assets: [
              {
                kind: "dip",
                asset_id: "windows-security-dip",
                version: "1.0.0",
                path: "dip/windows-security.json",
                sha256: "3aba6a23",
                dependencies: ["windows-security-uim"],
              },
            ],
            compatibility: { schema: "1" },
          },
          sha256: "0195fd13",
          state: "active",
          created_at: "2026-09-30T00:00:00Z",
          activated_at: "2026-09-30T01:00:00Z",
        },
      ],
    });
    expect(value.items[0]!.manifest.assets[0]!.dependencies).toHaveLength(1);
    expect(value.items[0]!.state).toBe("active");
  });

  it("parses the dataset catalog response", () => {
    const value = catalogSchema.parse({
      catalog_version: 1,
      active_generation: "g1",
      max_time_range_day: 31,
      datasets: [
        {
          name: "authentication",
          kind: "uim-domain",
          active_generation: "g1",
          index_pattern: "logs-ueba.authentication-<namespace>",
          quality_statuses: ["qualified", "partial"],
          fields: [
            { name: "ueba.quality.status", type: "keyword", sensitivity: "public", searchable: true, aggregable: true },
          ],
        },
        {
          name: "quarantine",
          kind: "quarantine",
          active_generation: "g1",
          index_pattern: "logs-ueba.quarantine-<namespace>",
        },
      ],
    });
    expect(value.datasets).toHaveLength(2);
    expect(value.datasets[1]!.quality_statuses).toEqual([]);
  });

  it("extracts stats buckets and totals from SPL results", () => {
    const stats = queryResultSchema.parse({
      mode: "stats",
      aggregations: {
        buckets: {
          after_key: { "ueba.quality.status": "qualified" },
          buckets: [{ key: { "ueba.quality.status": "qualified" }, doc_count: 7666 }],
        },
      },
    });
    expect(statsBuckets(stats)).toEqual([{ key: { "ueba.quality.status": "qualified" }, count: 7666 }]);
    const total = queryResultSchema.parse({ mode: "stats", aggregations: { count: { value: 42 } } });
    expect(statsTotal(total)).toBe(42);
    expect(statsTotal(queryResultSchema.parse({ mode: "stats" }))).toBe(0);
  });

  it("builds closed-set query request bodies", () => {
    expect(buildQueryBody("search authentication | head 5", undefined)).toEqual({
      query: "search authentication | head 5",
    });
    expect(
      buildQueryBody("search raw | head 5", "2026-10-01T00:00:00Z", {
        to: "2026-10-02T00:00:00Z",
        limit: 100,
        cursor: "abc",
      }),
    ).toEqual({
      query: "search raw | head 5",
      from: "2026-10-01T00:00:00Z",
      to: "2026-10-02T00:00:00Z",
      limit: 100,
      cursor: "abc",
    });
  });

  it("extracts single-value stats metrics including date values", () => {
    const result = queryResultSchema.parse({
      mode: "stats",
      aggregations: {
        count: { value: 126 },
        latest: { value: 1796000000000, value_as_string: "2026-10-12T10:00:00.000Z" },
      },
    });
    expect(statsMetric(result, "count")).toEqual({ value: 126, valueAsString: undefined });
    expect(statsMetric(result, "latest")).toEqual({
      value: 1796000000000,
      valueAsString: "2026-10-12T10:00:00.000Z",
    });
    expect(statsMetric(result, "missing")).toBeUndefined();
    const empty = queryResultSchema.parse({ mode: "stats", aggregations: { latest: { value: null } } });
    expect(statsMetric(empty, "latest")).toEqual({ value: null, valueAsString: undefined });
  });

  it("extracts provenance references from UIM events", () => {
    const value = eventProvenance({
      id: "evt-1",
      "@timestamp": "2026-10-12T10:00:00Z",
      event: { id: "evt-1", dataset: "authentication", outcome: "failure" },
      ueba: {
        route: { domain: "authentication", generation: "g1" },
        provenance: { raw_event_id: "raw-9f", release_id: "windows-security-1.0.0" },
        schema: { version: "1.0.0" },
        quality: { status: "qualified" },
      },
    });
    expect(value).toEqual({
      eventId: "evt-1",
      rawEventId: "raw-9f",
      releaseId: "windows-security-1.0.0",
      domain: "authentication",
      generation: "g1",
      sourceContextId: "",
      schemaVersion: "1.0.0",
      qualityStatus: "qualified",
    });
    expect(eventProvenance({}).rawEventId).toBe("");
  });

  it("parses export job lists including in-flight jobs", () => {    const value = exportsSchema.parse({
      items: [
        {
          id: "3f6b2c9e-0000-4000-8000-abcdefabcdef",
          job_id: "3f6b2c9e-0000-4000-8000-abcdefabcde0",
          dataset: "authentication",
          format: "csv",
          query: "search authentication | head 100",
          from: "2026-10-01T00:00:00Z",
          to: "2026-10-02T00:00:00Z",
          row_limit: 100000,
          byte_limit: 1073741824,
          include_sensitive: false,
          include_raw: false,
          state: "queued",
          created_at: "2026-10-02T00:00:00Z",
        },
      ],
      next_cursor: "",
    });
    expect(value.items[0]!.state).toBe("queued");
    expect(value.items[0]!.row_count).toBeUndefined();
  });

  it("accepts an entity-filtered anomaly page", () => {
    const value = anomalyPageSchema.parse({
      items: [
        {
          id: "anom-e1",
          type: "auth.failure-then-success",
          severity: "critical",
          status: "open",
          timestamp: "2026-10-12T01:00:00Z",
          score: 0.9,
          entity: { id: "ent:abc", type: "account" },
          rule_id: "auth.failure-then-success",
          rule_version: "1.0.0",
          summary: "多次失败后成功登录",
          evidence_count: 6,
        },
      ],
      next_cursor: "c",
      total: 1,
    });
    expect(value.items[0]!.entity.id).toBe("ent:abc");
    expect(value.total).toBe(1);
  });

  it("groups anomalies into entity aggregates", () => {
    const parse = (item: Record<string, unknown>) => anomalySummarySchema.parse(item);
    const aggregates = groupEntities([
      parse({
        id: "a1", type: "t", severity: "low", status: "closed",
        timestamp: "2026-10-10T00:00:00Z", entity: { id: "ent:1", type: "account" },
      }),
      parse({
        id: "a2", type: "t", severity: "critical", status: "open",
        timestamp: "2026-10-11T00:00:00Z", entity: { id: "ent:1", type: "account" },
      }),
      parse({
        id: "a3", type: "t", severity: "medium", status: "open",
        timestamp: "2026-10-12T00:00:00Z", entity: { id: "ent:2", type: "host" },
      }),
    ]);
    expect(aggregates).toHaveLength(2);
    expect(aggregates[0]).toMatchObject({
      id: "ent:2", type: "host", anomalyCount: 1, openCount: 1, highestSeverity: "medium",
    });
    expect(aggregates[1]).toMatchObject({
      id: "ent:1", anomalyCount: 2, openCount: 1, highestSeverity: "critical",
      latest: "2026-10-11T00:00:00Z",
    });
    expect(groupEntities([])).toEqual([]);
  });

  it("builds anomaly windows anchored to now", () => {
    const { from, to } = anomalyWindow(24);
    const delta = new Date(to).getTime() - new Date(from).getTime();
    expect(delta).toBe(24 * 3600 * 1000);
    expect(new Date(to).getTime()).toBeLessThanOrEqual(Date.now());
  });
});
