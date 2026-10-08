# TUBA 调查控制台

控制台使用 TUBA 自身的用户名/密码登录，通过同源 `/api/v1/auth/login` 建立 HttpOnly 服务器会话；不保存浏览器 access token，不连接 Keycloak。开发无需 VITE_OIDC 配置。账号初始化与部署见 [系统登录](../docs/SYSTEM-LOGIN.md)。

已实现路由：安全总览、异常列表/详情、案件列表/详情/创建、成员权限、系统运行和来源/采集器（管理面与采集面状态分列）。路由和操作按钮使用 `/api/v1/me` 返回的有效权限控制。列表查询通过 React Query 支持缓存、取消和错误恢复；响应由 Zod 校验。
