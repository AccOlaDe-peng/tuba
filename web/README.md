# TUBA 调查控制台

当前开发验收登录使用用户名和密码表单，通过 Keycloak `tuba-web` 客户端的 Direct Access Grant 换取 OIDC access token。提交后立即清空密码输入框；浏览器只在 sessionStorage 保存短期 access token，并只访问 Go API，不包含 Kafka、Elasticsearch 或数据库凭据。此简化流程仅用于开发验收，正式环境应使用 Authorization Code + PKCE 或企业身份联邦。

开发环境需要设置 `VITE_OIDC_ISSUER` 和 `VITE_OIDC_CLIENT_ID`。开发用 `tuba-web` public client 需开启 Direct Access Grants，并允许本机开发源访问 token endpoint。

已实现路由：安全总览、异常列表/详情、案件列表/详情/创建、成员权限和系统运行。路由和操作按钮使用 `/api/v1/me` 返回的有效权限控制。列表查询通过 React Query 支持缓存、取消和错误恢复；响应由 Zod 校验。
