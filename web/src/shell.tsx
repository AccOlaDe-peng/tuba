import { ReadableValue, readable } from "./readable";
import { Alert, Avatar, Button, Input, Layout, Menu, Spin, Tooltip, Modal, Empty } from "antd";
import type { MenuProps } from "antd";
import { useState, type FormEvent } from "react";
import {
  Activity,
  ArrowRight,
  FileSearch,
  FolderKanban,
  LayoutDashboard,
  ListTodo,
  Search,
  Database,
  RotateCcw,
  ChartNoAxesCombined,
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

import { routeTitles, routeParents, workbenchNavigation } from "./navigation";
import { useAuth } from "./auth";

export function AppShell() {
  const auth = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [searchOpen, setSearchOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [passwordOpen, setPasswordOpen] = useState(false);
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [passwordError, setPasswordError] = useState("");
  const [changingPassword, setChangingPassword] = useState(false);

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
            <h1>登录工作台</h1>
            <p>连接事件、实体与风险，在统一工作台完成研判和处置。</p>
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
            使用 TUBA 系统账号登录
          </div>
        </section>
        <aside className="login-signal" aria-hidden="true">
          <div className="brand"><span>T</span><div><strong>TUBA</strong><small>BEHAVIOR INTELLIGENCE</small></div></div>
          <div className="login-story"><span className="eyebrow">从行为线索，到可验证的判断。</span><h1>让每一次调查<br/>都有证据可循。</h1><p>连接事件、实体与风险，在统一工作台完成研判和处置。</p></div>
          <small>受控调查环境 · 身份验证与租户隔离</small>
        </aside>
      </main>
    );
  }

  if (location.pathname === "/") return <Navigate replace to={"/"+(workbenchNavigation.flatMap(g=>g.items).find(([, ,p])=>!p||auth.can(p))?.[0]??"overview")} />;

  const navIcons: Record<string, React.ReactNode> = {
    overview:<LayoutDashboard size={17}/>, anomalies:<Radar size={17}/>, risks:<ShieldCheck size={17}/>, entities:<Users size={17}/>, cases:<FolderKanban size={17}/>,
    events:<FileSearch size={17}/>,sources:<Radio size={17}/>,quality:<Sparkles size={17}/>,catalog:<Database size={17}/>,analysis:<ChartNoAxesCombined size={17}/>,baseline:<Activity size={17}/>,feedback:<ScrollText size={17}/>,
    releases:<Package size={17}/>,jobs:<ListTodo size={17}/>,replay:<RotateCcw size={17}/>,operations:<Activity size={17}/>,audit:<ScrollText size={17}/>,access:<Users size={17}/>
  };
  const availableGroups = workbenchNavigation.map(g=>({ ...g, items:g.items.filter(([, ,p])=>!p||auth.can(p)) })).filter(g=>g.items.length);
  const navItems: MenuProps['items'] = availableGroups.map(g=>({type:'group',label:g.group,children:g.items.map(([p,t])=>({key:'/'+p,label:t,icon:navIcons[p]}))}));
  const section = location.pathname.split("/")[1] ?? "overview";
  const detail =
    location.pathname.split("/").filter(Boolean).length > 1
      ? ` / ${decodeURIComponent(location.pathname.split("/").filter(Boolean).at(-1) ?? "")}`
      : "";
  const initials = readable(auth.principal.subject,"用户").slice(0, 2).toUpperCase();

  return (
    <Layout className="app-shell">
      <Modal title="修改密码" open={passwordOpen} confirmLoading={changingPassword} onCancel={() => {setPasswordOpen(false);setCurrentPassword("");setNewPassword("");setPasswordError("");}} onOk={() => {
        if (new TextEncoder().encode(newPassword).length < 12 || new TextEncoder().encode(newPassword).length > 256) {setPasswordError("新密码须为 12 至 256 字节");return;}
        setChangingPassword(true);setPasswordError("");
        void auth.changePassword(currentPassword,newPassword).then(() => {setPasswordOpen(false);setCurrentPassword("");setNewPassword("");}).catch((reason: Error) => setPasswordError(reason.message)).finally(() => setChangingPassword(false));
      }}>
        {passwordError && <Alert type="error" title={passwordError}/>}
        <label htmlFor="current-password">当前密码</label><Input.Password id="current-password" autoComplete="current-password" value={currentPassword} onChange={e=>setCurrentPassword(e.target.value)}/>
        <label htmlFor="new-password">新密码</label><Input.Password id="new-password" autoComplete="new-password" value={newPassword} onChange={e=>setNewPassword(e.target.value)}/>
      </Modal>
      <Layout.Sider
        className="command-nav"
        width={228}
        breakpoint="lg"
        collapsedWidth={72}
        theme="dark"
      >
        <div className="brand">
          <span>T</span>
          <div>
            <strong>TUBA</strong>
            <small>BEHAVIOR INTELLIGENCE</small>
          </div>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[`/${routeParents[section] ?? section}`]}
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
          <div className="workspace-scope"><small>工作空间</small><strong><ReadableValue value={auth.principal.organization_id} label="当前组织"/></strong><span>{auth.principal.namespace}</span></div>
          <Button className="workspace-search" icon={<Search size={14}/>} onClick={()=>setSearchOpen(true)}>搜索功能页面</Button>
          <div className="identity">
            <div>
              <strong><ReadableValue value={auth.principal.organization_id} label="当前组织"/></strong>
              <small><ReadableValue value={auth.principal.subject} label="当前用户"/></small>
            </div>
            <Avatar className="identity-avatar">{initials}</Avatar>
            <Button type="text" onClick={() => setPasswordOpen(true)}>修改密码</Button>
            <Tooltip title="退出登录">
              <Button
                type="text"
                aria-label="退出登录"
                icon={<LogOut size={17} />}
                onClick={() => void auth.logout()}
              />
            </Tooltip>
          </div>
        </Layout.Header>
        <Layout.Content className="content">
          {auth.error && <Alert className="global-alert" closable type="warning" showIcon title={auth.error} />}
          <Alert.ErrorBoundary>
            <div className="workspace-crumb"><span>工作空间</span><ArrowRight size={12}/><button onClick={()=>navigate('/'+(routeParents[section]??section))}>{routeTitles[section]??'调查'}</button>{detail&&<small>{detail}</small>}</div>
            <div className="page-frame">
              <Outlet />
            </div>
          </Alert.ErrorBoundary>
          <footer className="workspace-footer"><span>TUBA · 可解释行为分析与调查</span><span>权限与数据范围由服务端验证</span></footer>
        </Layout.Content>
      </Layout>
      <Modal title="快速跳转" open={searchOpen} onCancel={()=>setSearchOpen(false)} footer={null}>
        <Input autoFocus placeholder="搜索页面名称…" value={search} onChange={e=>setSearch(e.target.value)} allowClear/>
        <div className="page-search-results">{availableGroups.flatMap(g=>g.items).filter(([,t])=>t.includes(search.trim())).map(([p,t])=><button key={p} onClick={()=>{navigate('/'+p);setSearchOpen(false);setSearch('');}}>{t}<ArrowRight size={14}/></button>)}
        {!availableGroups.flatMap(g=>g.items).some(([,t])=>t.includes(search.trim()))&&<Empty description="没有匹配页面"/>}</div>
      </Modal>
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
