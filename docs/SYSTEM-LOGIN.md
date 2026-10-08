# TUBA 系统登录

2026-10-08 按用户确认修正身份边界：247 的 Keycloak 属于 Linux 认证及未来日志来源，与 TUBA 系统登录无关。TUBA 在自己的 Go API、PostgreSQL 和前端内完成账号登录。历史文档中的 247 realm、OIDC/JWKS、Direct Access Grant 与 `/tuba-auth` 登录流程均已退出当前产品。

## 登录与权限

- 前端只调用同源 `POST /api/v1/auth/login`，提交 JSON `username/password`。成功返回用户及有效权限，并设置 `tuba_session` Cookie；浏览器不再保存 access token，也不再连接 247。
- Cookie 为 HttpOnly、Secure、SameSite=Strict，限定 `/api/v1`。服务器只保存随机 256-bit 会话令牌的 SHA-256 摘要，默认 8 小时到期；重启后会话仍由数据库验证。
- 每次 API 请求复核身份是否停用、会话是否到期及数据库当前 membership/RBAC。角色、租户和 namespace 不来自用户登录输入。原有独立发布者授权继续有效。
- `POST /api/v1/auth/logout` 撤销当前会话；`POST /api/v1/auth/password` 校验旧密码、更新摘要并撤销该账号全部会话，前端要求重新登录。
- 密码长度 12–256 字节，PBKDF2-SHA256、600,000 次迭代、每个密码独立 128-bit 随机盐。连续 5 次密码错误锁定 15 分钟；锁定在数据库中持久化。登录/改密入口另有每连接来源每分钟 30 次的工作量限制，不信任客户端转发地址。
- Cookie 认证的写操作要求精确匹配 Origin；登录拒绝不同来源。命令行可从登录响应 Cookie 获取不透明会话值后用 Bearer 发送；旧外部 JWT 不再被接受。
- 登录成功、失败、锁定、无权限登录、退出、改密及创建账号写追加审计。审计与一般日志不包含密码、摘要或令牌。

## 首个管理员及旧账号迁移

迁移 `00022_local_system_login.sql` 增加 `local_accounts` 和 `auth_sessions`。数据库迁移和备份使用现有迁移流程。

首个账号使用 `tuba-bootstrap-operator --username admin --organization tenant_a --approved-by <授权引用>`，密码通过标准输入提供，不放在命令行、Git 或一般日志。该命令只在组织不存在有效本地管理员时工作。

从旧登录流程迁移时，可加 `--existing-subject <旧管理员 subject>`。命令验证其确实为指定组织的有效管理员，将原 identity 的 issuer 改为 `tuba:local` 并建立本地凭据；identity ID、subject、membership、案件引用和独立发布者引用全部保留，旧 issuer 进入迁移审计。它不操作 247 的服务或用户。

后续账号由租户管理员在「访问控制 → 添加成员」建立：`POST /api/v1/users` 同时建立本地账号和初始租户角色。角色调整/撤销仍通过 `/api/v1/members`。不再通过手工 SQL 或 Keycloak 创建系统用户。

## 部署设置

248 正常入口：`https://10.6.68.248:8443/tuba/`。

API 启动读取非敏感设置文件 `/etc/tuba/auth.json`，可通过 `TUBA_AUTH_CONFIG` 更换位置。这样只重启 API 即可调整登录设置，无需重启整条数据面。

```json
{"public_origin":"https://10.6.68.248:8443","cookie_secure":true,"session_ttl":"8h"}
```

`TUBA_PUBLIC_ORIGIN`、`TUBA_AUTH_COOKIE_SECURE`、`TUBA_AUTH_SESSION_TTL` 可显式覆盖；TTL 限制 1 分钟至 24 小时。仅本机 HTTP 开发设置 `cookie_secure=false`，并把 origin 设置为 Vite 的精确来源。Launcher 部署示例使用 HTTPS 与 Secure Cookie。

旧 8088 HTTP 开发入口改为跳转 HTTPS，不再代理 247。数据库备份包含系统账号摘要和会话；247 不属于 TUBA 系统身份备份。

## 验证

`go test ./internal/auth ./internal/api ./internal/control ./internal/config ./internal/webserver` 覆盖密码摘要、Cookie、退出/改密撤销、跨站写入拒绝和旧令牌拒绝。设置 `TUBA_LOGIN_TEST_DATABASE_URL` 后，`TestLocalLoginPostgres` 在随机临时 schema 中验证真实 PostgreSQL 上的账号迁移、权限保留、锁定恢复、账号创建与审计，结束删除测试 schema。

`python scripts/verify_api_es_identity.py` 使用独立 Compose 验证原生账号登录、成员撤销和 API 专用 ES key，不需要 Keycloak。

## 248 部署记录（2026-10-08）

已在 248 应用并登记 migration 00022。通过 bootstrap CLI 将旧管理员 subject `26a64c67-b05a-4d52-b08f-201a8657ec03` 迁移为系统用户名 `admin`，保留原 identity ID、tenant_a 角色和独立发布权限，写入 `auth.bootstrap_local_admin` 审计；密码随机生成，仅通过标准输入提供。247 服务/账号未变更。

API SHA-256：`35525f5ac05488d9bf7124decbd9c7e08ea25a41e741089cc85d1236bae72050`。仅通过 Launcher 重启 `api`，现场新 PID 1376046、restarts=0、readiness=200。前端原子切换到 `/opt/tuba/web/dist-native-login-20261008`；8088 返回 308 至 HTTPS，网关主配置无 `tuba-auth` 代理。

现场 21 项检查通过：匿名/错密拒绝、原生登录、原主体/租户/权限保留、Cookie 安全属性、刷新式会话读取、成员管理、跨站/无 Origin 写拒绝、旧外部令牌拒绝、多会话、改密、所有旧会话撤销、旧密码拒绝、新密码登录、退出与退出后拒绝、前端资产和旧登录代理退出。改密验证后最终初始密码保存在工作站 Git 忽略目录的权限 0600 文件，不进入仓库或部署日志。

变更前数据库备份：248 `/opt/tuba/native-login-20261008/backup/pre-login.dump`，581,656,382 bytes，SHA-256 `7b5d9f8233f8e6b1a482a515bf29c6220aebd6f380d9116dfdacf4cc9cc78258`；工作站 `output/native-login-20261008/pre-login.dump` 校验一致，pg_restore 目录可读。旧 API/bootstrap/manifest/HTTP 配置与前端目标保存在同一 root-only backup 目录，旧前端版本保留。回滚必须同时恢复旧 identity issuer（使用受审计的迁移工具或隔离恢复后取证）、旧 API/manifest 与前端；不能只恢复旧二进制后继续使用本地账号，也不能为登录回滚覆盖正在写入的整个数据库。

验证结果：登录相关 Go 测试与 vet 通过，真实 PG 随机 schema 集成测试通过；前端 typecheck、25 个 Vitest 测试与 `/tuba/` 构建通过；Linux/Windows API 与 bootstrap 构建通过；打包 Web 网关 smoke、安装依赖及初始化 shell 测试、OpenAPI 64 paths 验证通过。清理旧登录模块后 go mod tidy 未升级依赖。

全量门禁仍有与本次登录修改无关的限制：macOS 上 collector 的文件代次 int32 反射 panic、component/launcher 的 `/proc` 进程身份测试失败；零依赖合同校验器不支持既有 attributed-event schema 的 allOf，ES 原始模板已有生成漂移。本次未修改这些模块/事件合同/ES 资产。浏览器视觉验收被现有自签名证书的 `ERR_CERT_AUTHORITY_INVALID` 阻止，未绕过浏览器安全警告；HTTPS API 使用已有内网验证方式完成现场检查。

现场另见数据面部分服务处于重试/退避，根盘约 82–84% 已用；这些是独立运行问题，本次未重启或更改它们。数据库备份含真实数据，应按既有受限备份策略保管。

本机没有 Helm 和 Docker Compose V2，因此 helm-check 与 Compose 配置渲染未执行成功；该次 248 实装使用现有 Launcher 与网关，不使用 Helm/Compose。

## 8443 旧 DDR 入口移除（2026-10-08）

按用户要求，248 网关根路径 `/` 改为 302 跳转 `/tuba/`，不再加载 `/opt/adms/act/gui`。8443 上旧 `/sso`、`/adms`、`/adms/`、`/adms/api/`、`/monitor-api/` 及其他非 TUBA 页面返回 410。TUBA 前端、`/api/v1/` 与健康检查保留；共享数据库、旧程序文件和其他端口服务未删除。

配置备份：`/opt/tuba/native-login-20261008/backup/webserver-before-ddr-removal.conf`（0600）。网关配置检查与 reload 成功；实测根路径 302、TUBA 页面 200、未登录 API 401、readiness 200、旧页面及接口 410。
