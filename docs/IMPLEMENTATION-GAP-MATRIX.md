# TUBA 目标架构与当前代码差异矩阵

更新：2026-09-27  
基线：[TARGET-ARCHITECTURE.md](TARGET-ARCHITECTURE.md)；任务清单：[IMPLEMENTATION-TODO.md](IMPLEMENTATION-TODO.md)

本矩阵落实 A01：把每个目标服务映射到当前仓库代码，并标明复用、改造、新增或停用。这里的“已有”只表示存在代码落点，不代表阶段已经验收。优先级和完成条件以实施 TODO 为准。

## 分类

| 标记 | 含义 |
| --- | --- |
| 复用 | 已有实现符合目标合同；继续由该模块负责，不复制第二套生产逻辑 |
| 改造 | 已有代码承担目标职责，但合同、可靠性、运行方式或覆盖范围仍不完整 |
| 新增 | 目标职责没有可复用实现；需新增独立模块/命令及其状态模型 |
| 停用 | 旧职责退出生产链；如保留，仅用于诊断或历史验收 |

## 服务与能力映射

| 目标服务 / 能力 | 当前代码落点 | 分类 | 差异与后续实施任务 |
| --- | --- | --- | --- |
| 采集管理与适配 | 旧 `cmd/tuba-collector`、`internal/collector`；新增 `cmd/tuba-agent`（待建）、`cmd/tuba-source-adapter`、`internal/sourceadapter`、`deploy/components` | 改造 / 新增 | 旧 SQLite/Zeek reader/Sender 冻结；管理 CLI 改为组件代理；Beat 处理文件/Windows，source-adapter 首个 Kafka→内部 ingest receipt→手动 offset/DLQ 骨架已实现，ingest 按 Topic 名查询服务端 source context。source-adapter 有基础 Prometheus 计数器；生产配置生成、Topic ACL/token 生命周期管理、dashboard/告警与迁移验收尚未实现；另需 Syslog/API 接入包。对应新版 COL-01–COL-15。 |
| 可信接入 `tuba-ingest` | `cmd/tuba-ingest`、`internal/ingest`、`internal/rawevent` | 改造 | 已有来源凭证解析、限速、receipt 和 Raw Kafka 生产；补齐错误/就绪合同、source epoch/reset、Collector 固定上下文核验及持久故障验收。对应 I01–I03、COL-01。 |
| Raw Kafka 消费与证据索引 | `cmd/tuba-raw-indexer`、`internal/rawindexer`、`internal/sink/raw.go` | 改造 | 已有按接收 UTC 日期写 Raw ES 和 alias 的代码；复核 Raw 合同、访问权限、归档水位/过期状态、容量保护和恢复语义。对应 I04、I06、C07。 |
| DIP + UIM 标准化 `tuba-normalizer` | `cmd/tuba-normalizer`、`internal/normalizer`、`internal/uim` | 改造 | 进程与首批 Zeek/Windows 映射已存在；补齐不可变发布包/来源绑定、完整领域合同和字段能力、Kafka 事务/fencing、版本缓存、过滤结果及单条失败隔离。对应 N01–N05、C02、C09。 |
| 标准事件索引 | `cmd/tuba-standard-indexer`、`internal/standardindexer`、`internal/sink/uim_mapping.go` | 改造 | 已支持八领域、固定 UTC 日期索引、逐条 bulk 结果和冲突隔离；补齐模板/代次/alias 生命周期、retention 和运行配置合同。对应 N06–N08、C07。 |
| Quarantine 索引 | `cmd/tuba-quarantine-indexer`、`internal/quarantineindexer`、`contracts/events/quarantine/1` | 改造 | 隔离消费和写索引路径已存在；补齐稳定隔离合同、完整原始引用/失败字段/规则版本、索引模板、回查与修复流程。对应 N05、N07、C02、C07。 |
| 实体与归因 `tuba-entity-worker` | 当前无 `cmd/tuba-entity-worker`，无 `internal/entity*` | 新增 | 新建 Account/Device 身份空间、强弱标识解析、多角色 attribution、时态关系、inbox/state/checkpoint/outbox、PG 权威状态和 ES 投影。对应 E01–E05、C03、C06。 |
| 特征、基线、检测、风险 `tuba-analysis-worker` | `python/tuba_analysis`、`internal/analysisworker`、`internal/detection`、`cmd/tuba-detect-auth` | 改造 | 已有认证检测和 worker 基础；拆分在线 feature/baseline/detection/risk 模块，增加有界窗口、水位/迟到处理、去重、不可变版本及统一正式调度。`tuba-detect-auth` 保留诊断用途，不与正式 worker 并行产出。对应 F01–F08、R01–R02。 |
| 分析结果写入 `tuba-analysis-sink` | `cmd/tuba-analysis-sink`、`internal/sink/analysis.go`、`internal/analysis` | 改造 | 已有异常结果写入；扩展为多对象、revision/retracted、generation、稳定日期键、乱序保护及永久错误 DLQ。对应 C04、F06–F07。 |
| API 与授权 `tuba-api` | `cmd/tuba-api`、`internal/api`、`internal/auth`、`internal/control` | 改造 | OIDC/RBAC、案件、来源、Collector 管理、受限事件查询和总览已有落点；补齐发布/任务/实体/风险/Data Model API、SPL 受限计划、导出审计和权限边界验收。对应 C08、Q01–Q03、W04。 |
| 控制面任务与 outbox `tuba-control-worker` | `cmd/tuba-control-worker`、`internal/controlworker`、`internal/worker` | 改造 | 已有 outbox 发布器、租约/fencing、队列和重试框架；尚无完整业务 handler、任务/API 生命周期、发布流程、审计、清理及恢复验收。对应 T01–T05。 |
| Web 控制台 | `web/src`、`web/package.json` | 改造 | 登录、总览、异常、案件、部分 Collector/API 状态页面已存在；补齐来源/DIP/UIM/隔离/发布/任务/回放、实体风险调查、审计与备份状态，并逐项对齐 API 权限和空/错误状态。对应 W01–W05。 |
| PostgreSQL 事务与状态 | `migrations/00001_control_plane.sql` 至 `00011_collector_management.sql`、`internal/control` | 改造 | 已有组织/身份/案件/反馈、接入 receipt、worker lease、source credentials 与 Collector 管理表；复核逐表租户约束，补齐实体时态数据、发布包、质量状态及 retention。对应 C06、E03、T02、T05。 |
| Kafka 合同与客户端 | `contracts/events/topics.v1.json`、`internal/kafkautil` | 改造 | Topic 清单和基础 producer/consumer 已有；补齐 key/分区/ACL/事务参数合同、read_committed/fencing 证据及隔离环境运行验收。对应 C05、N01、T02–T03。 |
| Elasticsearch 索引/模板 | `elasticsearch/`、`internal/es`、`internal/sink`、各 indexer | 改造 | 已有索引客户端、八领域写入和部分模板；补齐公共/领域 schema 交叉检查、generation 清单、统一保留策略和迁移冲突处理。对应 C07、N06–N08。 |
| 身份服务 | `deploy/keycloak/tuba-realm.json`、`deploy/keycloak/tuba-realm-247-dev.json` | 复用 / 配置 | 使用现有 Keycloak；247 为开发 realm，用户名/密码授权只用于开发，生产使用标准 OIDC 流程。LDAP/AD 联邦和生产身份依赖另按部署要求配置。 |
| 单节点安装、升级、恢复 | `deploy/docker`、`deploy/helm`、旧 `deploy/collector`、`deploy/components`、`scripts` | 新增 / 改造 | 容器/Helm 与旧自研 Collector 脚本现存；新增 Beat 组件打包原型，但无管理 Agent、统一 Launcher、幂等初始化、版本切换、回滚和恢复手册。所有 TUBA 进程由产品 Launcher/CLI 管理，不注册 systemd 或 Windows Service。对应 O01–O03、COL-02/08/09、B03–B06。 |
| 观测与容量保护 | `internal/telemetry`、`deploy/helm/tuba/templates/observability.yaml`、`docs/OBSERVABILITY.md` | 改造 | 已有指标和 Helm 观测资源；单节点自管运行形态需补齐日志轮转、磁盘/保留保护、队列水位告警和可操作 Runbook。对应 O04–O05、I06、B06。 |

## 旧路径处置

| 旧路径 / 制品 | 处理方式 | 依据 |
| --- | --- | --- |
| `/api/v1/events/authentication` 专用入口与旧 indexer 路径 | 停用，不提供兼容；保留迁移说明和既有验收记录 | I05 已关闭 |
| `cmd/tuba-detect-auth` | 保留为诊断工具；生产规则只由 `tuba-analysis-worker` 调度 | 目标架构 §3、F08 |
| Helm 部署制品与 M0–M5 记录 | 保留作历史/可选部署证据；不能代表单节点产品安装、运行和恢复验收完成 | O01–O05、A05 |
| 原 Collector 试验脚本/临时接收器 | 只作历史演练证据；新采集迁移到受管 Beat/网关/连接器，现有 CLI/包改造为管理代理，旧队列排空后退役 | I03、COL-01–COL-10 |

## 合同和交付物差异

| 目标合同 / 交付物 | 当前落点 | 状态 / TODO |
| --- | --- | --- |
| Raw source envelope | `contracts/events/raw/1/schema.json`、`contracts/examples/raw.valid.json`、`internal/rawevent` | 单事件切片存在；source_position 重置、多记录、边界错误和来源生命周期仍待 C01/COL-01。 |
| UIM 八领域合同 | `contracts/uim/domain-contract.v1.yaml`、`contracts/uim/domain-catalog.v1.yaml` | 草案和首批映射存在；字段级语义、能力、缺字段和未知来源样例待 C02/N02–N04。 |
| 分析结果 revision/generation 合同 | `contracts/events/analysis-result/1/schema.json`、`internal/analysis/result.go` | 初版存在；多对象、撤回、稳定日期与迁移兼容待 C04/F06。 |
| Topic/key/ACL/事务合同 | `contracts/events/topics.v1.json` | Topic 初版存在；客户端事务支持和完整运行参数待 C05。 |
| PostgreSQL 完整权威模型 | `migrations/00001_control_plane.sql` 至 `00011_collector_management.sql` | 多个基础模型存在；目标表组复核、实体状态和 worker 完整协议待 C06。 |
| ES templates/aliases/generation/retention 清单 | `elasticsearch/` | 有部分领域模板；统一生成规则、别名生命周期和过期清理待 C07。 |
| API OpenAPI | `contracts/api/openapi.yaml` | 覆盖已实现 API；任务、发布、质量、实体、风险、查询语言及导出合同待 C08。 |
| 发布包 manifest 与依赖检查 | 暂无完整 manifest/validator | 新增 immutable release manifest、哈希与依赖兼容校验；对应 C09/T04。 |

## 实施顺序与风险

1. 先完成 A02 环境只读盘点、A03 容量约束和 A04 隔离代次决策，再把 C01–C09 变成可独立验收的合同。
2. 可复用的首批代码继续扩展，不新建重复 ingest、normalizer 或 indexer；实体 worker 与发布包校验是明确的新模块。
3. `python/tuba_analysis` 与 Go `cmd/tuba-detect-auth` 必须划清生产/诊断边界，避免同一规则双写异常。
4. 部署文档仍有历史 M0–M5、Helm 和当前自管进程要求；A05/O01 需统一产品交付表述后再关闭，不以某一种历史脚本替代正式 Launcher。


2026-09-27 采集路线修订：`tuba-source-adapter`、来源接入 Topic/ACL、受管 Beat 模板与多组件状态均为新增项；当前 source-adapter 已有需本地配置的首个处理骨架，ingest Topic/context 查询路由已接入，但不能作为生产闭环。原自研 Zeek reader/SQLite/Sender 为迁移保留，自研 Windows adapter 取消。历史矩阵中的 COL 编号按旧验收记录解释，实施按最新 TODO。
