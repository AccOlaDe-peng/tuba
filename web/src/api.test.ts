import { describe, expect, it } from "vitest";

import {
  anomalySummarySchema,
  caseSchema,
  casesSchema,
  catalogSchema,
  collectorsSchema,
  exportsSchema,
  queryResultSchema,
  queryString,
  releasesSchema,
  sourcesSchema,
  statsBuckets,
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

  it("parses export job lists including in-flight jobs", () => {
    const value = exportsSchema.parse({
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
});
