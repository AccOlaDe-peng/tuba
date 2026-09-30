# 开发环境与统一入口

> 采集方案于 2026-09-27 改为受管 Filebeat/Winlogbeat＋TUBA source-adapter。现有统一启动/打包脚本仍未管理新组件；adapter 可手动运行开发骨架，服务端配置下发和受管启动仍待 COL 任务完成。见 [采集设计](COLLECTOR-DESIGN.md)。


## 前置条件

- Go 1.27.1。`go` 命令可根据 `go.mod` 的 toolchain 指令自动获取该版本。
- uv 0.12.17；uv 根据 `.python-version` 获取 CPython 3.14.7。
- Node.js 24.14.1 和 Corepack；前端固定 pnpm 12.6.0。
- PyYAML，供 `make contracts` 的 OpenAPI 门禁（`scripts/validate_openapi.py`）解析 YAML 与 `$ref`。零依赖的基线校验器 `scripts/validate_contracts.py` 不需要它；两者分开正是为了让最小环境仍能跑基线。
- Docker Engine 与 Docker Compose V2，用于 Kafka、PostgreSQL、Elasticsearch 和可选 Keycloak。

不要把本地开发密码用于共享或生产环境。真实凭据只通过未提交的 `.env` 或密钥系统提供。

## 首次初始化

```bash
cd product
cp .env.example .env
make bootstrap
make dev-up
make check
make build
```

`make dev-up` 启动 Kafka、创建三个开发 topic，并启动 PostgreSQL 与 Elasticsearch。需要本地 Keycloak 时运行：

```bash
docker compose --profile identity up -d
```

Windows 环境可以直接运行：

```powershell
.\scripts\bootstrap_local.ps1
.\scripts\start_local.ps1
```

该脚本会创建 `.env`、启动全部依赖、等待健康检查、应用 PostgreSQL migrations 和 Elasticsearch assets。首次导入的本地 Keycloak 用户如下，密码统一为 `TubaLocal!123`：

| 用户名 | 角色 |
| --- | --- |
| `wang.min` | `tenant_admin` |
| `analyst.lee` | `analyst` |
| `auditor.zhao` | `viewer` |

本地 Keycloak 管理控制台为 `http://127.0.0.1:8180/admin/`，默认开发管理员密码是 `.env` 中的 `KEYCLOAK_ADMIN_PASSWORD`。

`start_local.ps1` 将 Go、Python 和 Web 进程以后台隐藏方式启动，PID 和日志位于 `.runtime/`。停止应用进程运行 `.\scripts\stop_local.ps1`，停止容器运行 `make dev-down` 或 `docker compose --profile identity down`。

本地辅助脚本入口（尚未构成完整架构验收）：

```powershell
uv run --project python python scripts\load_test.py --events 1000 --concurrency 20
.\scripts\fault_injection.ps1 -Events 50
.\scripts\security_check.ps1
.\scripts\backup_local.ps1
```

已有远程 Elasticsearch 或身份服务时，可以只运行需要的本地依赖，并在 `.env` 中覆盖连接配置。

需要在共享 Kafka 上做隔离验证时，可为 normalizer 与标准 indexer 设置 `KAFKA_EVENTS_TOPIC_PREFIX`（默认 `tuba.events`）和可选 `KAFKA_CONSUMER_GROUP_SUFFIX`；另用 `KAFKA_RAW_TOPIC`、`KAFKA_QUARANTINE_TOPIC`、`KAFKA_DLQ_TOPIC` 指向专用验证 topic。验证环境应使用单独的 ES namespace/alias，并确保主题 ACL 已配置，避免把测试事件写入正式消费链。标准 indexer 按 `INDEX_BATCH_SIZE`（默认 500）和 `INDEX_BATCH_BYTES`（默认 16 MiB，配置上限 64 MiB）共同限制批量，最多等待 `INDEX_BATCH_WAIT`（默认 1 秒），仅重试失败的临时 bulk 项；永久项进入 DLQ，成功或 DLQ 写入确认后才提交输入 offset。超出当前字节预算的下一条消息留在下一批处理，不会提前提交 offset。

## 常用命令

| 命令 | 用途 |
| --- | --- |
| `make contracts` | 校验 JSON Schema、示例与 UIM 用例（零依赖基线），并解析 OpenAPI：解析整个文档、递归解析全部 `$ref`（含外部 schema 文件并复跑同一套 schema 校验）、检查每个 path 的方法与 responses |
| `make go-check` | Go 测试和 vet |
| `make python-check` | Python 分析测试 |
| `make web-check` | TypeScript 检查和前端测试 |
| `make check` | 运行全部基础质量门禁 |
| `make build` | 构建 Go 命令、Python wheel/sdist 和 React 静态资源 |
| `make topics` | 幂等创建开发 topic |
| `make dev-down` | 停止本地依赖，保留 volume 数据 |

## 启动服务

加载 `.env` 后按需运行：

```bash
go run ./cmd/tuba-ingest
go run ./cmd/tuba-raw-indexer
go run ./cmd/tuba-standard-indexer
go run ./cmd/tuba-quarantine-indexer
go run ./cmd/tuba-analysis-sink
go run ./cmd/tuba-api
go run ./cmd/tuba-source-adapter # 需配置 SOURCE_ADAPTER_CONFIG、SOURCE_ADAPTER_TOKEN 与 adapter DLQ topic
uv run --project python tuba-analysis-worker
corepack pnpm@12.6.0 --dir web dev
```

`tuba-source-adapter` 读取 `deploy/components/source-adapter.example.json` 的同结构配置，配置只声明已登记的来源 Topic。`SOURCE_ADAPTER_TOKEN` 必须是至少 32 字符的内部共享令牌；ingest 端使用相同的 `SOURCE_ADAPTER_TOKEN`，并通过 `DATABASE_URL` 查询 Topic 对应的启用来源实例和不可变 source context。Kafka 连接通过 `KAFKA_BROKERS` 和 `KAFKA_SECURITY_*` 提供。开发时需手动创建来源 Topic 和 `tuba.source-adapter.dlq.v1`，配置最小读写 ACL，并创建 source_context 与 Topic 命名相符的来源登记。Topic 未绑定、token 错误或服务端返回任意 HTTP 错误时，adapter 保留来源 offset 并持续退避重试；只有 adapter 本地判定为无效的 Beat 消息会先写 DLQ 再提交 offset。Management Agent 尚未生成或下发此配置，token 发放/轮换和 Kafka ACL 自动编排也未实现。

Adapter 指标端点由 `SOURCE_ADAPTER_METRICS_LISTEN` 配置，默认 `127.0.0.1:9185`，可通过 `/metrics` 抓取。首批指标包括已读取/接收/拒绝事件数、投递重试数、DLQ 写入及失败数、offset 提交及失败数；计数器随进程重启归零，尚无持久水位或告警规则。

来源注册 API 为每个 Zeek dataset 返回独立 `source_context_id`。Topic/ACL 初始化工具读取 PostgreSQL 中已启用的 context，并按固定命名生成单分区、单副本、24 小时保留的 Topic，以及来源写入和 adapter 读取的精确 ACL。默认只输出计划，只有显式 `-apply` 才修改 Kafka；运行时需要 `DATABASE_URL`、`KAFKA_BROKERS` 和具备管理权限的 `KAFKA_SECURITY_*`。示例：

```bash
go run ./cmd/tuba-source-topic-admin -context ctx_0123456789abcdef0123456789abcdef -source-principal User:tuba-zeek-conn -adapter-principal User:tuba-source-adapter
go run ./cmd/tuba-source-topic-admin -context ctx_0123456789abcdef0123456789abcdef -source-principal User:tuba-zeek-conn -adapter-principal User:tuba-source-adapter -apply
```

该工具要求 broker 已启用 ACL authorizer，且 Kafka 用户已由管理员安全创建；不创建 SCRAM 用户，也不发布 Filebeat 配置。248 上当前 broker 通告地址为 `localhost:9192`、无 ACL authorizer，21 无法用此状态直接完成采集闭环。需增加 21 可达的私网 listener、完成现有客户端 ACL 盘点并启用 authorizer，再执行上述 apply；不能直接重启共享 broker 以跳过盘点。

当前 Web 登录页直接向配置的开发 Keycloak realm 提交用户名和密码，realm 需允许 `tuba-web` 的 Direct Access Grants；API 仍校验返回的访问令牌并基于 PostgreSQL 成员关系授权。该简化登录仅用于当前开发验收，不代表生产身份接入已定版。既有 M1–M5 UI/认证记录见历史验收文档，不作为完整目标架构的交付证明。

### 247 开发环境首个租户管理员引导

247 的开发 realm 与本机 local-dev realm 是不同 issuer。首个 247 操作者必须先在 Keycloak 创建用户，再通过 `cmd/tuba-bootstrap-operator` 为实际 Keycloak `sub` 建立首个 `tenant_admin` 成员关系。该一次性工具要求目标 issuer 下尚无有效 tenant administrator；它在同一 PostgreSQL 事务内建立 identity/membership 并写入 `identity.bootstrap_membership` 审计事件，`actor_identity_id` 为空表示这是无现存登录操作者时的显式 bootstrap，`approved_by` 记录外部授权依据。已有管理员时工具会拒绝执行，后续成员变更必须走受保护的 `/api/v1/members` API。不要手工 SQL 修改成员关系。

开发环境 Keycloak 用户可由 `scripts/provision_247_dev_operator.py` 创建。脚本只从环境读取 Keycloak bootstrap admin 凭据，生成随机密码并仅打印一次；密码不落盘。将用户 `sub`、issuer 和组织传给引导工具后，使用生成的用户名/密码登录。当前工作站的 Vite OIDC 参数保存在被 Git 忽略的 `web/.env.local`；访问 248 loopback API 时还需受控 SSH 本地转发 `127.0.0.1:8788 -> 248:127.0.0.1:8788`。

## CI

根目录 `.gitlab-ci.yml` 使用与本地相同的 Go、Python 和 Node 主版本，执行合同检查、Go/Python/前端测试及 Go 构建。后续里程碑会增加 OpenAPI/JSON Schema 兼容、安全扫描、镜像构建和部署验证。
