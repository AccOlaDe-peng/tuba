import { describe, expect, it } from "vitest";

import { anomalySummarySchema, caseSchema, casesSchema, collectorsSchema, queryString, sourcesSchema } from "./api";

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
});
