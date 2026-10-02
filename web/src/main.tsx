import { lazy, Suspense, StrictMode, type ReactNode } from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App as AntApp, ConfigProvider, Spin } from "antd";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import "antd/dist/reset.css";

import { AuthProvider } from "./auth";
import { AppShell, RequirePermission } from "./shell";
import "./styles.css";

const load = <K extends keyof typeof import("./pages")>(name: K) =>
  lazy(async () => {
    const module = await import("./pages");
    return { default: module[name] };
  });

const Overview = load("Overview");
const Anomalies = load("Anomalies");
const AnomalyDetail = load("AnomalyDetail");
const Cases = load("Cases");
const CaseDetail = load("CaseDetail");
const Operations = load("Operations");
const Sources = load("Sources");
const Quality = load("Quality");
const Releases = load("Releases");
const Jobs = load("Jobs");
const Access = load("Access");

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
        { path: "overview", element: wait(<Overview />) },
        { path: "anomalies", element: guard("anomaly:read", <Anomalies />) },
        { path: "anomalies/:id", element: guard("anomaly:read", <AnomalyDetail />) },
        { path: "cases", element: guard("case:read", <Cases />) },
        { path: "cases/:id", element: guard("case:read", <CaseDetail />) },
        { path: "operations", element: guard("operations:read", <Operations />) },
        { path: "sources", element: guard("source:manage", <Sources />) },
        { path: "quality", element: guard("event:read", <Quality />) },
        { path: "releases", element: guard("release:read", <Releases />) },
        { path: "jobs", element: guard("event:read", <Jobs />) },
        { path: "access", element: guard("user:manage", <Access />) },
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
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: "#0d746b",
          colorInfo: "#246b86",
          colorSuccess: "#24765b",
          colorWarning: "#a66f22",
          colorError: "#b53e42",
          colorText: "#172321",
          colorTextSecondary: "#5e6d69",
          colorBgLayout: "#eef1ed",
          colorBorderSecondary: "#d9e0dc",
          borderRadius: 8,
          fontFamily: '"IBM Plex Sans", "Microsoft YaHei UI", "Noto Sans CJK SC", sans-serif',
          controlHeight: 36,
        },
        components: {
          Layout: {
            headerBg: "#f8faf7",
            bodyBg: "#eef1ed",
          },
          Menu: {
            darkItemBg: "#112624",
            darkSubMenuItemBg: "#112624",
            darkItemSelectedBg: "#1e3d39",
            darkItemHoverBg: "#193330",
            itemBorderRadius: 6,
          },
          Table: {
            headerBg: "#f4f7f4",
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
