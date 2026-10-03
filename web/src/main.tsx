import { lazy, Suspense, StrictMode, type ReactNode } from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App as AntApp, ConfigProvider, Spin } from "antd";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import "antd/dist/reset.css";
import zhCN from "antd/locale/zh_CN";

import { AuthProvider } from "./auth";
import { AppShell, RequirePermission } from "./shell";
import "./styles.css";
import "./workbench.css";

const load = <K extends keyof typeof import("./pages")>(name: K) =>
  lazy(async () => {
    const module = await import("./pages");
    return { default: module[name] };
  });

const Overview = load("Overview");
const Events = load("Events");
const Anomalies = load("Anomalies");
const AnomalyDetail = load("AnomalyDetail");
const Entities = load("Entities");
const EntityDetail = load("EntityDetail");
const Cases = load("Cases");
const CaseDetail = load("CaseDetail");
const Operations = load("Operations");
const Quality = load("Quality");
const Jobs = load("Jobs");
const Access = load("Access");
const Audit = load("Audit");

const loadWorkbench = <K extends keyof typeof import('./workbench')>(name:K)=>lazy(async()=>({default:(await import('./workbench'))[name]}));
const Catalog=loadWorkbench("Catalog");
const Risks=loadWorkbench("Risks");
const Baseline=loadWorkbench("Baseline");
const Analysis=loadWorkbench("Analysis");
const Feedback=loadWorkbench("Feedback");
const SourcesWorkbench=loadWorkbench("SourcesWorkbench");
const SourceDetail=loadWorkbench("SourceDetail");
const Agents=loadWorkbench("Agents");
const AgentDetail=loadWorkbench("AgentDetail");
const ReleaseDetail=loadWorkbench("ReleaseDetail");
const Backups=loadWorkbench("Backups");
const Replay=loadWorkbench("Replay");
const JobDetail=loadWorkbench("JobDetail");
const ReleaseList=loadWorkbench("ReleaseList");
const Publishers=loadWorkbench("Publishers");
const ExportDetail=loadWorkbench("ExportDetail");

function wait(value: ReactNode) {
  return (
    <Suspense
      fallback={
        <div className="page-loading">
          <Spin size="large" />
        </div>
      }
    >
      {value}
    </Suspense>
  );
}

function guard(permission: string, value: ReactNode) {
  return <RequirePermission permission={permission}>{wait(value)}</RequirePermission>;
}

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <AppShell />,
      children: [
        { path: "overview", element: guard("anomaly:read", <Overview />) },
        { path: "events", element: guard("event:read", <Events />) },
        { path: "anomalies", element: guard("anomaly:read", <Anomalies />) },
        { path: "anomalies/:id", element: guard("anomaly:read", <AnomalyDetail />) },
        { path: "entities", element: guard("anomaly:read", <Entities />) },
        { path: "entities/:id", element: guard("anomaly:read", <EntityDetail />) },
        { path: "cases", element: guard("case:read", <Cases />) },
        { path: "cases/:id", element: guard("case:read", <CaseDetail />) },
        { path: "operations", element: guard("operations:read", <Operations />) },
        { path: "sources", element: guard("source:manage", <SourcesWorkbench />) },
        { path: "quality", element: guard("event:read", <Quality />) },
        { path: "releases", element: guard("release:read", <ReleaseList />) },
        { path: "jobs", element: guard("event:read", <Jobs />) },
        { path: "access", element: guard("user:manage", <Access />) },
        { path: "publishers", element: guard("release:manage", <Publishers />) },
        { path: "risks", element: guard("anomaly:read", <Risks />) },
        { path: "catalog", element: guard("event:read", <Catalog />) },
        { path: "analysis", element: guard("release:read", <Analysis />) },
        { path: "baseline", element: guard("anomaly:read", <Baseline />) },
        { path: "feedback", element: guard("analysis:feedback", <Feedback />) },
        { path: "sources/:id", element: guard("source:manage", <SourceDetail />) },
        { path: "agents", element: guard("source:manage", <Agents />) },
        { path: "agents/:id", element: guard("source:manage", <AgentDetail />) },
        { path: "releases/:id", element: guard("release:read", <ReleaseDetail />) },
        { path: "backups", element: guard("operations:read", <Backups />) },
        { path: "replay", element: guard("event:read", <Replay />) },
        { path: "exports/:id", element: guard("event:read", <ExportDetail />) },
        { path: "jobs/:id", element: guard("event:read", <JobDetail />) },
        { path: "*", element: <section className="empty-state"><h1>页面不存在</h1><a href={window.TUBA_CONFIG?.basePath ?? '/'}>返回工作台</a></section> },
        { path: "audit", element: guard("user:manage", <Audit />) },
      ],
    },
  ],
  { basename: window.TUBA_CONFIG?.basePath ?? "/" },
);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (attempt, error) => {
        if (attempt >= 1) return false;
        return !(error instanceof Error && /403|404|没有权限/.test(error.message));
      },
      staleTime: 15_000,
      refetchOnWindowFocus: false,
    },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ConfigProvider locale={zhCN}
      theme={{
        token: {
          colorPrimary: "#087f80",
          colorInfo: "#246b86",
          colorSuccess: "#24765b",
          colorWarning: "#a66f22",
          colorError: "#b53e42",
          colorText: "#19333c",
          colorTextSecondary: "#70828a",
          colorBgLayout: "#f3f5f5",
          colorBorderSecondary: "#e0e7e8",
          borderRadius: 7,
          fontFamily: '"Segoe UI Variable Text", "Microsoft YaHei UI", sans-serif',
          controlHeight: 36,
        },
        components: {
          Layout: {
            headerBg: "#ffffff",
            bodyBg: "#f3f5f5",
          },
          Menu: {
            darkItemBg: "#11262d",
            darkSubMenuItemBg: "#11262d",
            darkItemSelectedBg: "#24474e",
            darkItemHoverBg: "#193330",
            itemBorderRadius: 6,
          },
          Table: {
            headerBg: "#f5f8f8",
            rowHoverBg: "#f2f8f6",
            borderColor: "#e2e8e4",
          },
          Tag: {
            borderRadiusSM: 4,
          },
        },
      }}
    >
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <AuthProvider>
            <RouterProvider router={router} />
          </AuthProvider>
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  </StrictMode>,
);
