# TUBA — AI 代理项目指南

> 本文件面向不了解本项目的 AI 编码代理。所有内容以仓库实际代码和文档为准；目标与当前完成度的权威口径是 `docs/TARGET-ARCHITECTURE.md` 与 `docs/IMPLEMENTATION-TODO.md`。文档与文档索引见 `docs/README.md`。

## 项目概述

TUBA 是面向 Windows Security 与 Zeek 等来源的 UEBA（用户与实体行为分析）平台。仓库正在从早期认证事件纵切迁移到完整单节点架构：**README 中"未交付"清单（TUBA Management Agent 远程配置下发、来源过滤策略、任务/outbox、实体、特征、风险与管理查询控制台）仍未完成**，不要把代码切片和历史 M1–M5 验收记录当作完整产品已交付。逐项差异见 `docs/IMPLEMENTATION-GAP-MATRIX.md`。

目标数据流：

```text
来源 → Beat/网关/连接器 → Kafka 接入 → source-adapter → tuba-ingest → Kafka Raw
                              ├→ Raw Indexer → Elasticsearch 原始证据
                              └→ Normalizer（DIP + UIM）→ 标准事件 Kafka
                                                       ├→ 标准 Indexer → Elasticsearch
                                                       └→ Entity / Feature / Detection / Risk workers
```

采集方案已定：Filebeat/Winlogbeat、Syslog 网关和专用连接器，由 TUBA Management Agent 统一管理；现有自研采集代码（`internal/collector`）冻结新增通用采集需求，仅保留迁移用途。

## 技术栈（版本以 `TECH-STACK-BASELINE.md` 为权威）

| 层 | 选型 |
| --- | --- |
| Go（主流程） | Go 1.27.1（`go.mod` 模块名 `tuba/product`，语言基线 1.27.0）；标准库 `net/http`（无 Web 框架） |
| Go 依赖 | `segmentio/kafka-go` 0.4.51、`jackc/pgx/v5` 5.11.0、`coreos/go-oidc/v3` + `golang.org/x/oauth2` |
| Python（分析） | CPython 3.14.* + uv；`confluent-kafka` 2.15.1、NumPy、pandas、SciPy、scikit-learn、psycopg（见 `python/pyproject.toml`） |
| 前端 | React 19.3 + TypeScript 6.0.3 + Vite 8.3；Ant Design 6、ECharts 6、React Router 7、TanStack Query 5、Zod 4、lucide-react；Node 24.14.1 + pnpm 12.6.0（仅构建工具链，经 corepack 调用） |
| 中间件 | Kafka 4.3.1（KRaft）、Elasticsearch 8.19.22、PostgreSQL 18.6、Keycloak 26.7.4（OIDC Auth Code + PKCE） |
| SQL 迁移 | `pressly/goose`，仅 SQL migration，位于 `migrations/`（序号前缀，当前到 `00022_*.sql`） |
| 部署 | Kubernetes + Helm chart（`deploy/helm/tuba`）；Docker Compose V2 仅用于本地开发 |
| 可观测性 | OpenTelemetry、Prometheus、Grafana；日志用 Go `log/slog` |

语言边界：Go 负责高吞吐接入、Kafka 主链路、在线 API、RBAC、CLI；Python 只用于批量/窗口分析与统计/机器学习；浏览器不直连 ES/Kafka/PostgreSQL，只通过 Go API（OIDC 登录）。**明确暂不引入**：Redis、Flink/Spark/Kafka Streams、Schema Registry、Kafka Connect/Logstash、服务网格、独立图数据库（条件与理由见技术栈基线）。

## 目录结构

- `cmd/tuba-*` — 22 个 Go 命令入口：`tuba-ingest`（Raw 接入）、`tuba-raw-indexer` / `tuba-standard-indexer` / `tuba-quarantine-indexer`、`tuba-normalizer`、`tuba-source-adapter`、`tuba-api`、`tuba-web`、`tuba-agent`、`tuba-collector`、`tuba-launcher`、`tuba-control-worker`、`tuba-entity-worker`、`tuba-detect-auth`、`tuba-analysis-sink` / `tuba-analysis-catchup`、`tuba-bootstrap-operator`（首个租户管理员引导）、`tuba-source-topic-admin` / `tuba-topic-admin` / `tuba-kafka-security-admin`、`tuba-component`、`tuba-bootstrap-release-publisher`。
- `internal/` — 按领域划分的 Go 包，与 cmd 一一对应或提供共享能力：`ingest`、`rawindexer`、`standardindexer`、`quarantineindexer`、`normalizer`、`uim`（八领域统一信息模型）、`sourceadapter`、`api`、`auth`、`control` / `controlworker`、`agent`、`collector`、`launcher`、`entity` / `entityworker`、`detection`、`analysis` / `analysisworker`、`sink`、`es`（受限 `net/http` ES 封装）、`event` / `rawevent`、`kafkaadmin` / `kafkautil` / `kafkarevoke`、`pgutil`、`config`、`spl`、`telemetry`、`webserver`、`worker`、`lifecycle`、`deadletter`、`catalog`、`component`、`indexing`。
- `python/tuba_analysis/` — Python 分析包，入口脚本 `tuba-analysis-worker` / `tuba-analysis-replay` / `tuba-analysis-evaluate`；测试在 `python/tests/`，评估场景在 `python/scenarios/`。
- `web/src/` — React SPA（`@tuba/web`）：`api.ts`、`auth.tsx`、`pages.tsx`、`components.tsx` 等；`web/.env.local`（Git 忽略）保存 Vite OIDC 参数。
- `contracts/` — **机器可读合同（单一事实来源）**：`api/openapi.yaml`（OpenAPI 3.1）、`events/<域>/<主版本>/schema.json`（版本化 JSON Schema）、`events/topics.v1.json`（Topic 目录）、`uim/`（领域目录与 Normalizer 验证用例）、`releases/1/manifest.schema.json`（语义发布包）、`examples/`（Go/Python/CI 共用的 Golden examples）、`ids.md`（稳定 ID 规则）。
- `migrations/` — goose SQL 迁移（22 个），全部 schema 变更入库、可审阅。
- `elasticsearch/` — ES index/component template 与 ILM JSON 资产（由 `scripts/generate_es_templates.py` 生成/校验）。
- `deploy/` — `helm/tuba` chart、`keycloak/local-dev` realm、`collector`、`components`（如 `source-adapter.example.json`）、`launcher`、`observability`、`profiles`、`docker`、`validation`。
- `releases/windows-security-1.0.0/` — 语义发布包（DIP/UIM/分析/路由/ES 资产 + manifest）。
- `scripts/` — 大量 ps1/sh/py 运维与验证脚本（安装、备份/恢复、验证 verify_*、打包 package_*）；`scripts/lib/` 为公共库。
- `dist/` — 各验证批次的构建产物与验收记录，不是源代码。
- `docs/` — 全部设计/基线/环境盘点文档（中文）。`docs/history/` 为历史记录。
- `.runtime/` — `start_local.ps1` 启动的本地进程 PID 与日志。

## 构建与测试命令

统一入口是根目录 `Makefile`（注意：Windows 上 Bash 工具为 Git Bash；Windows 原生操作用 `scripts/*.ps1`）：

| 命令 | 用途 |
| --- | --- |
| `make bootstrap` | `uv sync --project python --frozen` + `corepack pnpm@12.6.0 --dir web install --frozen-lockfile` |
| `make contracts` | 合同门禁：`scripts/validate_contracts.py`（零依赖基线）+ `validate_openapi.py`（需 PyYAML，递归解析 `$ref`）+ `generate_es_templates.py` |
| `make helm-check` | `helm lint deploy/helm/tuba` + `helm template` 渲染校验 |
| `make go-check` | `go test ./...` + `go vet ./...` |
| `make python-check` | `uv run --project python python -m unittest discover -s python/tests` + 场景评估（`--min-precision 1 --min-recall 1`） |
| `make shell-check` | 安装器/初始化器/领养预检的 shell 测试（用 fake 依赖，需 bash、python3、curl，不需要真实中间件） |
| `make web-check` | `tsc -b` 类型检查 + `vitest run --passWithNoTests` |
| `make check` / `make test` | 全部质量门禁（contracts + helm-check + go-check + python-check + shell-check + web-check） |
| `make build` | `go build ./cmd/...` + `uv build --project python` + `corepack pnpm@12.6.0 --dir web build` |
| `make dev-up` / `make dev-down` | 启动/停止 Compose 依赖（Kafka、PostgreSQL、Elasticsearch；`--profile identity` 含 Keycloak） |
| `make topics` | 幂等创建开发 Topic（`docker compose run --rm kafka-init`） |
| `make clean` | 删除 `web/dist` 与 `python/dist` |

首次初始化（Windows）：`copy .env.example .env`，然后 `.\scripts\bootstrap_local.ps1` 与 `.\scripts\start_local.ps1`（脚本会启动依赖、等待健康检查、应用 PG migrations 与 ES assets；进程 PID/日志在 `.runtime/`，停止用 `.\scripts\stop_local.ps1`）。本地 Keycloak 在 `http://127.0.0.1:8180/admin/`，示例用户（`wang.min`/`analyst.lee`/`auditor.zhao`，密码见 `docs/DEVELOPMENT.md`）仅限本机使用。

按需单独运行服务：`go run ./cmd/tuba-ingest` 等（完整列表见 `docs/DEVELOPMENT.md`）；前端开发 `corepack pnpm@12.6.0 --dir web dev`；Python worker `uv run --project python tuba-analysis-worker`。

## 开发约定与代码风格

- **文档主语言为中文**，代码注释与提交说明跟随现有风格。
- 所有依赖锁定精确版本，禁止 `latest`；Go module / uv.lock / pnpm lockfile 改动需同步 `TECH-STACK-BASELINE.md` 与部署 values。
- Go HTTP 服务只用标准库 `net/http`，不引入 Web 框架；ES 访问走仓库内受限 `net/http` 封装（`internal/es`），不同时维护两套 ES 客户端。
- 合同演进规则：同一主版本只允许兼容性增加；破坏性变更必须新建主版本目录与对应 topic/API 版本。Kafka Topic 命名固定 `tuba.<层>.<领域>.v<合同主版本>`，生产禁用自动建 Topic。
- 配置通过环境变量/`.env` 注入（`.env.example` 为模板）；部署参数集中配置，禁止散落在代码常量。
- 索引器语义（示例，见 DEVELOPMENT.md）：标准 indexer 按 `INDEX_BATCH_SIZE`（500）/`INDEX_BATCH_BYTES`（16 MiB）/`INDEX_BATCH_WAIT`（1s）成批，永久失败项进 DLQ，成功或 DLQ 确认后才提交 offset。
- 共享 Kafka 验证时用 `KAFKA_EVENTS_TOPIC_PREFIX` / `KAFKA_CONSUMER_GROUP_SUFFIX` 等环境变量隔离，避免污染正式消费链。

## 测试策略

- Go：`go test ./...`（各 `internal/` 包内有 `*_test.go`）。
- Python：`python/tests/` 下 unittest + `python/scenarios/*.json` 的 Golden Scenario 评估（precision/recall 门槛均为 1）。
- 前端：Vitest（`web/src/*.test.ts(x)`），`--passWithNoTests`。
- Shell：`scripts/test_*.sh` 用 fake 依赖驱动安装器/初始化器，无需真实中间件。
- 合同/Golden examples：`contracts/examples/` 由 Go、Python 和合同门禁共用；改 schema 必须同步示例与 `uim/validation-cases.v1.json`。
- 端到端验证：`scripts/verify_*.py|ps1|sh` 系列针对具体验收项（ACL、密钥、备份恢复、打包等）。
- 文档提到根目录 `.gitlab-ci.yml`，但**当前仓库中该文件不存在**；以 `make check` 为准。

## 安全注意事项

- 真实凭据只通过未提交的 `.env`（已 gitignore）或密钥系统提供；**禁止在镜像、Git、日志和 Compose 默认值里放真实凭据**；本地开发密码不得复用到共享/生产环境。不要读取或外泄 `.env`。
- 日志/trace 中不写 token、API key 或原始敏感字段。
- 认证由 Keycloak（OIDC）完成，TUBA 不保存密码；授权以 PostgreSQL 中 `tenant_membership`/RBAC 为权威，默认拒绝，不信任客户端传入的租户/namespace。不要手工 SQL 修改成员关系——首个管理员用 `cmd/tuba-bootstrap-operator`，之后走受保护的 `/api/v1/members` API。
- `SOURCE_ADAPTER_TOKEN` 是至少 32 字符的内部共享令牌；source-adapter 仅在本地判定消息无效时先写 DLQ 再提交 offset，其他失败保留 offset 退避重试。
- `tuba-source-topic-admin` 等管理工具默认只输出计划，显式 `-apply` 才修改 Kafka；不创建 SCRAM 用户。
- 敏感操作（登录、授权拒绝、导出、规则发布、密钥轮换等）写追加式审计记录并关联 request/trace ID。

## 部署

- 本地开发：Docker Compose V2 单节点（`compose.yaml`），Kafka `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`，Topic 由 `kafka-init` 预创建。
- 生产目标：Kubernetes + Helm（`deploy/helm/tuba`，`make helm-check` 做 lint/template 校验）、Gateway API + Envoy Gateway、Vault；生产 Kafka 至少 3 broker、RF=3、`acks=all`、TLS/SASL、ACL。镜像构建见 `scripts/build_images.ps1`，安装/回滚见 `scripts/install_tuba_linux.sh`、`scripts/rollback_*`。
- 备份/恢复：`scripts/backup_local.ps1`、`backup_elasticsearch.ps1`、`restore_*.ps1`、`backup_tuba_to_offsite.sh`。

## 常见坑

- 文档与现实的差距是设计的一部分：动手实现前先对照 `docs/IMPLEMENTATION-TODO.md` 的未勾选项与 `docs/IMPLEMENTATION-GAP-MATRIX.md`，README 的"当前实现切片"一节明确列出了未交付项（Agent 客户端未接通控制面、管理控制台未交付等）。
- `docs/`、`TECH-STACK-BASELINE.md`、`compose.yaml` 等部分文件使用 CRLF 行尾，编辑时保留。
- `make contracts` 的 `validate_openapi.py` 需要系统 Python 装 PyYAML；`validate_contracts.py` 零依赖可单独跑。
- 248/247/21 等数字指具体环境主机，相关现状以 `docs/ENVIRONMENT-248-REPORT.md`（指定日期快照）为准。
