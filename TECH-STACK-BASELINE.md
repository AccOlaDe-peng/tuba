# TUBA 技术栈基线

**状态：正式版基线已确定**  
**决策日期：2026-09-24**  
**适用范围：** `product/` 下正式版服务、任务、CLI、前端、部署与运行依赖。Go 是主流程和在线服务的主要语言；Python 是分析计算的正式语言；前端使用 React + TypeScript。`implementation/` 和 `docs/` 中较早的实现结构作为领域/数据合同参考，不代表产品运行架构。

## 决策原则

- Go 负责高吞吐接入、Kafka 主链路、在线 API、权限控制和服务编排。Python 仅用于适合其生态的离线/窗口分析和统计/机器学习，不负责高吞吐接入和通用在线 API。
- Web 控制台采用 React + TypeScript 独立构建，通过 Go API 访问产品；Node.js 仅为前端构建工具链，不是线上业务后端。
- 生产、预发、开发共用同一组主版本；开发环境只允许降低副本数，不允许换中间件或换消息语义。
- 所有容器镜像、Go module、Python 包和前端依赖必须锁定到精确版本，禁止 `latest`。补丁升级通过依赖更新 PR、兼容检查和回归后发布。
- Elasticsearch 当前已有 8.x 资产，因此正式版锁在 8.19 系列，不在第一阶段跨大版本升级。

## 正式选型

| 层 | 选型与版本 | 作用 / 约束 |
| --- | --- | --- |
| Go 主流程工具链 | Go 1.27.1；`go.mod` 语言基线 1.27.0 | 接入服务、Kafka 生产/消费主链路、索引器、在线 API、RBAC、任务编排、CLI 与管理程序。 |
| 分析运行时 | CPython 3.14.7；uv 0.12.17 | Python 用在批量/窗口分析、特征工程、统计基线、异常检测和实验评估。以 `pyproject.toml`、`uv.lock` 固定依赖。 |
| Python 数据计算 | NumPy 2.5.3、pandas 3.0.6、SciPy 1.18.1、scikit-learn 1.9.1 | 只按算法需要加入；不将 DataFrame/机器学习库放入接入和在线 API 服务。纯规则/简单统计仍优先用 Go，减少跨语言运维。 |
| Python Kafka 客户端 | `confluent-kafka` 2.15.1 | 基于 librdkafka 的分析 consumer/producer；消费标准事件并发布分析结果。 |
| HTTP 服务 | Go 标准库 `net/http` | 暂不引入 Web 框架；统一中间件顺序、超时、请求 ID、限流和恢复处理。 |
| Python 内部分析 API | 首期不单独部署 | 分析模块默认以 Kafka worker 或 Go 控制面调度的批任务运行。确有低延迟模型服务需求时，再针对该服务选择 FastAPI 等框架。 |
| Web UI | React 19.3.0、TypeScript 6.0.3、Vite 8.3.0、`@vitejs/plugin-react` 6.1.1 | 独立 SPA 构建，覆盖查询、时间线、图表、关系探索、案件和管理工作台。 |
| 前端组件与数据层 | Ant Design 6.6.5、ECharts 6.1.0、React Router 7.18.4、TanStack Query 5.103.2、Zod 4.6.5 | 统一组件、图表、路由、服务端状态和运行时数据校验；浏览器不包含 ES 查询凭据。 |
| 前端运行时/包管理 | Node.js 24.14.1 LTS；pnpm 12.6.0 | 仅构建/测试前端，不作为生产业务后端；以 lockfile 固定完整依赖树。 |
| 消息总线 | Apache Kafka 4.3.1，KRaft 模式；Go 客户端 `segmentio/kafka-go` 0.4.51 | 首期即为接入、索引、分析解耦和重放的主链路。生产至少 3 broker，RF=3、`min.insync.replicas=2`、生产 `acks=all`、TLS/SASL、ACL；开发 Compose 单节点不代表生产拓扑。 |
| 日志与分析索引 | Elasticsearch 8.19.22；现有 Kibana 与其保持同版 | 标准事件、实体、特征、基线、异常、风险、案件证据及检索。使用 Data Stream、模板、ILM、稳定文档 ID 和批量索引；禁止由客户端任意构造 DSL。 |
| Elasticsearch Go 接入 | 现阶段沿用仓库内受限 `net/http` 客户端；封装层维持与 Elasticsearch 8 API 兼容 | 目前仓库只有小型 REST 封装。出现复杂 typed API、重试或 bulk 管理需求时改用官方 `go-elasticsearch/v8` 8.19.7；不同时维护两套 ES 客户端。 |
| 事务元数据 | PostgreSQL 18.6；Go 驱动 `jackc/pgx/v5` 5.11.0 | 用户档案、租户与成员关系、角色授权、数据源配置、规则发布记录、审计元数据、任务状态与幂等/调度元数据。PostgreSQL 是这些事务数据的权威源；ES 不承担账户与授权关系的权威存储。 |
| SQL 迁移 | `pressly/goose` 3.28.0，SQL migration only | 所有 schema 变更入库、可审阅、可回滚或前滚；避免依赖 Go 代码 migration。 |
| 用户登录与身份源 | Keycloak 26.7.4，OIDC Authorization Code + PKCE | TUBA 不保存密码；企业已有 IAM 时通过 OIDC/LDAP 联邦接入。Keycloak 提供认证；TUBA 自己的 PostgreSQL RBAC 负责资源授权。 |
| Go OIDC 校验 | `coreos/go-oidc/v3` 3.20.0 + `golang.org/x/oauth2` 0.33.0 | 基于 discovery/JWKS 校验 issuer、audience、签名和时效。移除自写 JWT/JWKS 密码学验证。 |
| 可观测性 SDK | OpenTelemetry Go traces/metrics API+SDK 1.46.0；日志继续用 Go `log/slog` | 服务统一 trace、关键业务指标和结构化日志，日志/trace 中不写 token、API key 或原始敏感字段。 |
| 可观测性采集 | OpenTelemetry Collector Contrib 0.161.0 | 收集 OTLP traces/metrics/logs；导出给 Prometheus、Grafana 和集中日志存储。 |
| 指标与仪表板 | Prometheus 3.13.3 LTS + Grafana OSS 13.2.1 | Prometheus LTS 管平台与服务指标；Grafana 展示 Kafka lag、丢弃/DLQ、ES bulk/query 延迟、检测新鲜度、租户配额等。Prometheus 不存业务日志。 |
| 生产编排 | Kubernetes 1.36.4；Helm 4.3.0 | 部署 Go 服务、Python worker、React 静态前端和运维组件，支持滚动发布、探针、资源限额、HPA/PDB。先做可移植 OCI 镜像；工作负载不得假设有本机持久盘。 |
| 入口与 TLS | Kubernetes Gateway API + Envoy Gateway 1.9.1 | 对外 TLS 终止、路由和基础流量治理；应用 API 仍做认证、授权、租户隔离与审计。 |
| 密钥 | 生产 Vault 1.21.7；开发用环境变量/未提交的本地 `.env` | 密钥运行时注入，轮换 API key、OIDC client secret 和 Kafka 凭据；禁止在镜像、Git、日志和 Compose 默认值里放真实凭据。 |
| 镜像与本地开发 | OCI 镜像；Docker Compose V2；Kafka 开发镜像 `apache/kafka:4.3.1` | Compose 用单节点 Kafka 做开发和联调。生产镜像用非 root 用户、只读根文件系统、固定 digest 与 SBOM。Go/Python/前端分别构建、分别发布，分析任务单独扩缩容。 |
| 静态检查 | `golangci-lint` 2.13.2；`go vet`、`gofmt`、`govulncheck` | CI 固定版本并阻止不合格构建。CI Runner 的基础 Go 镜像锁 `golang:1.27.1`。 |

## 首期必须部署的组件

1. **Kafka**：认证日志主事件主题、标准事件主题、无效事件/DLQ 主题；预过滤只移除合同明确不需要的数据，丢弃数和原因必须可观测。Kafka 是耐久缓冲和回放边界，不是无差别垃圾桶。
2. **Elasticsearch**：现有日志检索与分析底座；单独压测摄取、查询与聚合，不能用增加 Kafka 容量掩盖 ES mapping、分片或查询问题。
3. **PostgreSQL**：多租户产品控制面与事务元数据，包括用户映射、授权、数据源/规则管理、审计与任务状态。
4. **Keycloak/OIDC**：登录、MFA/身份联邦和令牌签发；如果企业 IAM 已提供 OIDC，则接企业身份源，可不单独运行 Keycloak 实例。
5. **可观测性**：OpenTelemetry Collector、Prometheus、Grafana；生产不能只靠机器日志排障。
6. **Kubernetes、Gateway、Vault**：正式生产目标环境。单机试点可暂用 Compose/系统服务，但其部署形态属于试点，不改变产品运行架构。

Kafka topic 命名固定 `tuba.<层>.<领域>.v<合同主版本>`；每个领域至少有 ingest/raw、normalized、invalid/DLQ 的清晰职责。开发 topic 可以低分区、RF=1；生产分区数、保留时长、压缩、配额与容量需通过目标吞吐、最大事件大小、恢复点目标和磁盘预算压测计算，部署参数集中配置，禁止散落在代码常量。生产初始不启用自动建 topic。

## 明确暂不引入

| 组件 | 决定 | 理由 / 重新评估条件 |
| --- | --- | --- |
| Python 高吞吐接入/主 API | 不用于这两类路径 | Go 负责接入和在线控制面；Python 专注分析优势区。 |
| Redis | 首期不部署 | 暂无必须外置的缓存、会话或分布式锁需求；先用 PostgreSQL、Kafka 与进程内有界缓存。实测缓存命中或共享限流需要后再加。 |
| Flink、Spark、Kafka Streams | 不作为首期流计算平台 | Go consumer 负责主链路，Python worker 负责分析；只有现有 worker 的窗口状态、吞吐或恢复能力经压测确认不足时再评估专用流计算平台。 |
| Schema Registry | 暂不部署 | 首期事件合同采用仓库版本化 JSON Schema 与 CI 兼容检查；多个异构生产者/消费者或 Protobuf 后再考虑。 |
| Kafka Connect/Logstash | 不作为产品核心接入层 | 统一由 Go 接入边界做认证、限流、租户绑定和预过滤；若具体数据源接入成本高，可单独评审 connector，而不能绕开统一身份与租户校验。 |
| 服务网格 | 首期不部署 | Kubernetes 网络策略、Gateway、应用 TLS 和 Kafka ACL 足够；出现多服务双向身份/策略需求再评估。 |
| 独立图数据库/在线特征库 | 暂不部署 | 先将实体关系与分析特征落在 PostgreSQL/Elasticsearch，并用压测确定是否需要专用存储。 |

## 账户、租户与权限边界

- Keycloak 的 realm/group/用户身份可以联邦企业目录；TUBA 在 PostgreSQL 保存 `user_profile`、`tenant`、`tenant_membership`、`role`、`permission` 和资源授权映射。保存外部 `sub`/issuer，不复制密码。
- Go API 从已验证的 OIDC 主体解析用户身份；角色和租户成员关系以 PostgreSQL 当前授权数据为准，不信任任意客户端传入的组织或 namespace。
- 采用租户级 RBAC + 资源级策略，默认拒绝；敏感操作单独授权。索引服务使用有限权限身份；用户不能直接获得共享 Elasticsearch 凭据。
- 管理员、登录、授权拒绝、导出、规则发布、案件更新和数据源密钥轮换写入追加式审计记录，并关联 request/trace ID。

## 语言边界与数据契约

- Go → Kafka 的事件合同以版本化 JSON Schema 为准；Kafka key 使用 tenant + entity/主体 ID，敏感字段按合同脱敏或剔除。
- Python 分析 worker 消费 normalized topic，支持 consumer group、offset/checkpoint、事件时间水位、迟到数据回补和有界批量；结果通过结果 topic 或 Go 控制的受限写入接口返回。第一期建议结果写入 topic，由 Go sink 统一写 Elasticsearch，避免 Python 与 Go 两套 ES 写入规范。
- 分析结果必须带 `event/window/model/rule version`、租户、稳定结果 ID、证据引用和运行 ID，按幂等键重复执行不产生重复异常。
- Python worker 独立镜像、依赖锁、资源配额和扩缩容策略；模型训练/实验环境与线上分析 worker 隔离，禁止 notebook 作为生产任务调度器。
- 浏览器对 Go API 使用 OIDC 登录；React 前端不直连 Elasticsearch、Kafka 或 PostgreSQL。复杂图表、时间线/关系图优先在前端交互渲染，查询仍由 Go 授权和编译。

## 与当前代码的衔接

- `product/go.mod` 锁 Go 1.27.1 工具链及当前 Kafka Go 客户端；Python 3.14.7、uv、Node 24 和前端 lockfile 已接入。
- Go API 使用标准 OIDC 库验证 token，租户成员与资源权限以 PostgreSQL membership/RBAC 为权威。
- Compose 用于本地开发；完整 Helm chart、Gateway、Vault、NetworkPolicy、HPA/PDB、监控告警和备份制品已交付。目标生产集群仍需完成 HA 中间件部署、容量、故障切换和 RPO/RTO 签字。
- Elasticsearch 版本选择与现有 8.x 资产保持兼容，先核对当前服务器具体 patch，再按 Elastic 8.19 升级路径维护；官方 Elasticsearch 8 Go 客户端在实际切换时锁到同一 `8.19.x` 最新补丁。

## 版本更新规则

每季度检查 Go/Go module、中间件的安全公告与受支持分支；高危安全修复可提前更新。Go 和 module 更新需通过单元、集成、Golden Scenario、租户越权、故障恢复及代表性吞吐回归。Kafka、PostgreSQL、Elasticsearch、Keycloak 的大版本升级必须做数据备份恢复演练和可回滚升级验证。任何 patch 更新都同步修改 `go.mod`、Compose/部署 values 和本表，确保文档与可执行配置一致。
