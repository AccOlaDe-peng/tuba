# React 调查控制台

## 路由

| 路由 | 页面 | 权限 |
| --- | --- | --- |
| `/overview` | 安全总览 | 已登录 |
| `/anomalies` | 异常调查 | `anomaly:read` |
| `/anomalies/:id` | 异常详情、证据和规则反馈 | `anomaly:read`；反馈需 `analysis:feedback` |
| `/cases` | 案件中心 | `case:read` |
| `/cases/:id` | 案件处置与活动 | `case:read`；写入需 `case:write` |
| `/operations` | 系统运行、分析 run 和 checkpoint | `operations:read` |
| `/access` | 用户与权限 | `user:manage` |

前端路由守卫只用于改善交互。Go API 每次请求仍执行 系统会话验证、数据库 membership 复核和权限检查。

## 身份会话

- 通过同源 `/api/v1/auth/login` 使用系统账号登录；不连接外部身份服务。
- 会话由 HttpOnly、Secure、SameSite=Strict Cookie 承载，浏览器不保存 access token。退出与改密在服务器撤销会话。
- `401` 会触发全局会话清理；`403` 保留会话但阻止页面或动作。
- 用户组织、namespace、角色和权限以 `/api/v1/me` 的响应为准。

## API 与查询

- 所有浏览器请求统一经过 `/api/v1`，服务端地址由网关或 Vite 开发代理提供。
- 响应使用 Zod 做运行时合同校验；错误同时兼容 JSON 错误体和网关文本错误。
- 列表使用游标分页。异常和案件页面使用无限查询累积结果，不使用 offset 分页。
- React Query 管理缓存和取消信号；页面卸载或筛选变化会取消未完成请求。
- 浏览器不持有 Elasticsearch、Kafka 或 PostgreSQL 凭据，也不直接查询数据中间件。

## 证据与敏感字段

- 异常详情只展示规则解释、实体、分数和经 API 白名单返回的证据事件。
- 案件详情展示关联异常 ID、状态、指派和审计活动，不返回原始凭据或 secret。
- 组件对空状态、加载状态、权限拒绝、数据冲突和依赖不可用提供独立反馈。

## 布局与可访问性

- 桌面端使用固定调查导航和密集数据面板，移动端导航折叠为图标栏。
- 主要命令使用文字或图标加文字；纯图标操作提供 tooltip 和 `aria-label`。
- 状态、严重度和角色除颜色外同时使用文字标签。
- 页面动画遵循 `prefers-reduced-motion`。

## 验证

```bash
corepack pnpm@12.6.0 --dir web typecheck
corepack pnpm@12.6.0 --dir web test
corepack pnpm@12.6.0 --dir web build
```

M3 的浏览器验收记录见 [`M3-ACCEPTANCE.md`](M3-ACCEPTANCE.md)。
