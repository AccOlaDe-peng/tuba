# 身份、权限与租户安全模型

## 信任边界

TUBA 内置账号与服务器会话证明“用户是谁”；TUBA PostgreSQL 证明“用户当前属于哪些租户、拥有哪些产品权限”；Go API 对每次请求执行最终授权。浏览器中的路由守卫只改善体验，不构成安全控制。

数据源使用独立 service identity/API credential。接入服务根据该身份绑定 tenant、namespace、允许的日志类型和配额，忽略 payload 中试图覆盖这些边界的字段。

## 首版授权模型

| 角色 | 核心权限 |
| --- | --- |
| `platform_admin` | tenant 管理、平台配置和运行状态；不自动读取租户事件 |
| `tenant_admin` | 本租户成员、数据源、规则启停、配额和审计读取 |
| `analyst` | 本租户事件/异常读取、案件创建与更新、反馈 |
| `viewer` | 本租户异常和案件只读 |
| `auditor` | 经批准范围内的审计和调查记录只读 |
| `publisher` | 合同、规则、模型发布；不自动读取租户数据 |

权限使用动作式名称，例如 `event:read`、`anomaly:read`、`case:write`、`member:manage`、`rule:publish`、`export:create`。API 按 permission 授权，不在处理器里散落角色名称判断。

## PostgreSQL 权威数据

首版至少包含 tenant、user_profile、identity、membership、role、permission、role_permission、membership_role、service_identity、api_credential_metadata、audit_event 和 idempotency_record。所有租户数据表显式包含 `tenant_id` 并建立相应唯一键/外键；代码查询必须从授权上下文注入 tenant ID。

账号密码使用带独立随机盐的 PBKDF2-SHA256 摘要；服务器只保存随机会话令牌的 SHA-256 摘要。每次请求验证会话有效期、身份启用状态及 PostgreSQL 当前 membership/RBAC；退出撤销当前会话，改密撤销全部会话，成员撤销立即影响后续授权。Cookie 使用 HttpOnly、Secure、SameSite=Strict，写操作校验精确 Origin。247 Keycloak 属于外部 Linux 认证及未来日志来源，与系统登录无关。配置、锁定、引导及迁移见 [系统登录](SYSTEM-LOGIN.md)。

## 服务与数据权限

- 每个 Go/Python 服务使用独立 Kafka principal、数据库用户和 ES API key。
- Kafka ACL 限制具体 topic 和 consumer group；Python worker不能消费 raw 凭据 topic，也不能写事件 topic。
- ES 写入身份按索引类型隔离；API 查询身份只允许所需 data stream/index alias。普通用户不获得任何共享 ES 凭据。
- PostgreSQL 连接必须 TLS；数据库角色分 migration、application 和只读运维用途。
- 密钥由受限权限的环境文件或部署平台注入；Vault 仅是后续可选实现，不是当前依赖。日志、trace、错误、DLQ 和审计记录不得包含 secret/token。

## 审计

登录结果、授权拒绝、成员/角色变更、数据源凭据操作、规则/模型发布、导出、案件状态和证据变更必须审计。审计记录包含 actor、tenant、action、resource、result、request/trace ID、来源 IP/客户端、时间和安全的变更摘要。审计是追加式数据，应用不得提供普通更新/删除接口。

## 安全验收

发布前至少验证：跨租户 ID 猜测、列表/详情/导出越权、角色提升、失效成员、伪造 tenant claim、错误 audience/issuer、过期 token、Kafka topic 越权、ES alias 绕过、日志/DLQ 泄密、重复幂等键和高并发案件更新。
