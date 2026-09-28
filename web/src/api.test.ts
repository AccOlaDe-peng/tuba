import { describe, expect, it } from "vitest";

import { anomalySummarySchema, caseSchema, casesSchema, queryString } from "./api";

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
});
