import { Alert, Avatar, Button, Input, Layout, Menu, Spin, Tooltip } from "antd";
import type { MenuProps } from "antd";
import { useState, type FormEvent } from "react";
import {
  Activity,
  ArrowRight,
  FileSearch,
  FolderKanban,
  LayoutDashboard,
  ListTodo,
  LogOut,
  Package,
  Radar,
  Radio,
  ScrollText,
  ShieldCheck,
  Sparkles,
  Users,
} from "lucide-react";
import { Navigate, Outlet, useLocation, useNavigate } from "react-router-dom";

import { useAuth } from "./auth";

const routeTitles: Record<string, string> = {
  overview: "安全总览",
  events: "事件查询",
  anomalies: "异常调查",
  entities: "实体画像",
  cases: "案件中心",
  operations: "系统运行",
  sources: "来源与采集器",
  quality: "数据质量",
  releases: "版本发布",
  jobs: "任务与回放",
  access: "用户与权限",
  audit: "审计事件",
};

export function AppShell() {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submitLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const submittedPassword = password;
    setPassword("");
    setSubmitting(true);
    try {
      await auth.login(username.trim(), submittedPassword);
    } finally {
      setSubmitting(false);
    }
  }

  if (auth.loading) {
    return (
      <div className="boot-screen">
        <div className="boot-mark">T</div>
        <Spin size="large" />
        <span>正在建立安全会话</span>
      </div>
    );
  }

  if (!auth.principal) {
    return (
      <main className="login-screen">
        <section className="login-panel">
          <div className="login-brand">
            <span>T</span>
            <div>
              <strong>TUBA</strong>
              <small>Threat Investigation Console</small>
            </div>
          </div>
          <div className="login-copy">
            <span className="eyebrow">受控调查环境</span>
            <h1>认证事件调查控制台</h1>
            <p>租户身份、权限与数据范围由后端授权模型确定。</p>
          </div>
          {auth.error && <Alert type="error" showIcon title={auth.error} />}
          <form className="login-form" onSubmit={(event) => void submitLogin(event)}>
            <label htmlFor="login-username">用户名</label>
            <Input
              id="login-username"
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="输入用户名"
              required
            />
            <label htmlFor="login-password">密码</label>
            <Input.Password
              id="login-password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="输入密码"
              required
            />
            <Button
              type="primary"
              size="large"
              htmlType="submit"
              loading={submitting}
              icon={<ShieldCheck size={18} />}
            >
              登录
            </Button>
          </form>
          <div className="login-foot">
            <span className="live-dot" />
            用户名与密码由身份服务验证
          </div>
        </section>
        <aside className="login-signal" aria-hidden="true">
          <div className="signal-grid" />
          <div className="signal-line line-a" />
          <div className="signal-line line-b" />
          <div className="signal-core">AUTH</div>
        </aside>
      </main>
    );
  }

  if (location.pathname === "/") return <Navigate replace to="/overview" />;

  const navItems: MenuProps["items"] = [
    {
      type: "group",
      label: "调查",
      children: [
        { key: "/overview", label: "安全总览", icon: <LayoutDashboard size={17} /> },
        ...(auth.can("event:read")
          ? [{ key: "/events", label: "事件查询", icon: <FileSearch size={17} /> }]
          : []),
        ...(auth.can("anomaly:read")
          ? [
              { key: "/anomalies", label: "异常调查", icon: <Radar size={17} /> },
              { key: "/entities", label: "实体画像", icon: <Users size={17} /> },
            ]
          : []),
        ...(auth.can("case:read")
          ? [{ key: "/cases", label: "案件中心", icon: <FolderKanban size={17} /> }]
          : []),
      ],
    },
    {
      type: "group",
      label: "运营",
      children: [
        ...(auth.can("operations:read")
          ? [{ key: "/operations", label: "系统运行", icon: <Activity size={17} /> }]
          : []),
        ...(auth.can("source:manage")
          ? [{ key: "/sources", label: "来源与采集器", icon: <Radio size={17} /> }]
          : []),
        ...(auth.can("event:read")
          ? [
              { key: "/quality", label: "数据质量", icon: <Sparkles size={17} /> },
              { key: "/jobs", label: "任务与回放", icon: <ListTodo size={17} /> },
            ]
          : []),
      ],
    },
    {
      type: "group",
      label: "管理",
      children: [
        ...(auth.can("release:read")
          ? [{ key: "/releases", label: "版本发布", icon: <Package size={17} /> }]
          : []),
        ...(auth.can("user:manage")
          ? [
              { key: "/access", label: "用户与权限", icon: <Users size={17} /> },
              { key: "/audit", label: "审计事件", icon: <ScrollText size={17} /> },
            ]
          : []),
      ],
    },
  ];

  const section = location.pathname.split("/")[1] ?? "overview";
  const detail =
    location.pathname.split("/").filter(Boolean).length > 1
      ? ` / ${decodeURIComponent(location.pathname.split("/").filter(Boolean).at(-1) ?? "")}`
      : "";
  const initials = auth.principal.subject.slice(0, 2).toUpperCase();

  return (
    <Layout className="app-shell">
      <Layout.Sider
        className="command-nav"
        width={252}
        breakpoint="lg"
        collapsedWidth={72}
        theme="dark"
      >
        <div className="brand">
          <span>T</span>
          <div>
            <strong>TUBA</strong>
            <small>Investigation</small>
          </div>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[`/${section}`]}
          items={navItems}
          onClick={({ key }) => navigate(key)}
        />
        <div className="nav-foot">
          <span className="live-dot" />
          <div>
            <strong>租户隔离已启用</strong>
            <small>{auth.principal.namespace}</small>
          </div>
        </div>
      </Layout.Sider>
      <Layout className="workspace">
        <Layout.Header className="topbar">
          <div className="breadcrumb">
            <span>TUBA</span>
            <ArrowRight size={13} />
            <strong>{routeTitles[section] ?? "调查"}</strong>
            {detail && <small>{detail}</small>}
          </div>
          <div className="identity">
            <div>
              <strong>{auth.principal.organization_id}</strong>
              <small>{auth.principal.subject}</small>
            </div>
            <Avatar className="identity-avatar">{initials}</Avatar>
            <Tooltip title="退出登录">
              <Button
                type="text"
                aria-label="退出登录"
                icon={<LogOut size={17} />}
                onClick={auth.logout}
              />
            </Tooltip>
          </div>
        </Layout.Header>
        <Layout.Content className="content">
          {auth.error && <Alert className="global-alert" closable type="warning" showIcon title={auth.error} />}
          <Alert.ErrorBoundary>
            <div className="page-frame">
              <Outlet />
            </div>
          </Alert.ErrorBoundary>
        </Layout.Content>
      </Layout>
    </Layout>
  );
}

export function RequirePermission({
  permission,
  children,
}: {
  permission: string;
  children: React.ReactNode;
}) {
  return useAuth().can(permission) ? (
    children
  ) : (
    <section className="access-denied">
      <ShieldCheck size={34} />
      <h1>当前角色无权访问</h1>
      <p>请求已由前端路由和后端权限策略双重限制。</p>
    </section>
  );
}
