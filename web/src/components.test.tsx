import { beforeAll, describe, expect, it } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import type { ReactElement } from "react";

let SeverityTag: (props: { value: string }) => ReactElement;
let AnomalyStatusTag: (props: { value: string }) => ReactElement;
let CaseStatusTag: (props: { value: string }) => ReactElement;

beforeAll(async () => {
  // components.tsx transitively imports auth.tsx, which reads window.TUBA_CONFIG
  // at module scope; the test runtime is node without a DOM.
  (globalThis as { window?: unknown }).window = { TUBA_CONFIG: {} };
  const module = await import("./components");
  SeverityTag = module.SeverityTag;
  AnomalyStatusTag = module.AnomalyStatusTag;
  CaseStatusTag = module.CaseStatusTag;
});

// Unknown backend enum values must render as-is (muted), never be
// re-labelled as a known state like "开放"/"中危".
describe("status/severity tag fallbacks", () => {
  it("renders known values with their labels", () => {
    expect(renderToStaticMarkup(<SeverityTag value="critical" />)).toContain("紧急");
    expect(renderToStaticMarkup(<AnomalyStatusTag value="investigating" />)).toContain("调查中");
    expect(renderToStaticMarkup(<CaseStatusTag value="in_progress" />)).toContain("调查中");
  });

  it("renders unknown severity as-is instead of defaulting to 中危", () => {
    const html = renderToStaticMarkup(<SeverityTag value="catastrophic" />);
    expect(html).toContain("catastrophic");
    expect(html).not.toContain("中危");
  });

  it("renders unknown anomaly status as-is instead of defaulting to 开放", () => {
    const html = renderToStaticMarkup(<AnomalyStatusTag value="escalated" />);
    expect(html).toContain("escalated");
    expect(html).not.toContain("开放");
  });

  it("renders unknown case status as-is instead of defaulting to 待分派", () => {
    const html = renderToStaticMarkup(<CaseStatusTag value="reopened" />);
    expect(html).toContain("reopened");
    expect(html).not.toContain("待分派");
  });

  it("covers every backend enum value for anomaly and case status", () => {
    for (const status of ["open", "investigating", "closed", "false_positive"]) {
      expect(renderToStaticMarkup(<AnomalyStatusTag value={status} />)).not.toContain("未知");
    }
    for (const status of ["open", "in_progress", "closed"]) {
      expect(renderToStaticMarkup(<CaseStatusTag value={status} />)).not.toContain("未知");
    }
    for (const severity of ["low", "medium", "high", "critical"]) {
      expect(renderToStaticMarkup(<SeverityTag value={severity} />)).not.toContain("未知");
    }
  });
});
