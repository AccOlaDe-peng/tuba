# TUBA 完整架构实施 TODO

日期：2026-09-25｜采集方案修订：2026-09-27 v2｜基线：[TARGET-ARCHITECTURE.md](TARGET-ARCHITECTURE.md)

本清单覆盖完整单节点产品目标。未勾选项仍未按本基线验收；旧 M0–M5 完成记录不能直接关闭新任务。仓库和 248 开发环境包含多阶段代码切片并完成过部分隔离/登录验收，但不代表所有切片都已部署或整阶段通过；只有达到任务完成条件的条目才勾选。

状态覆盖说明：A03 已于 2026-09-28 定版并关闭。下方较早验收记录中“待 A03”“A03 未定”或旧 7d/14d Kafka 目标仅保留当时背景，不再表示当前状态；当前权威边界是 Kafka 24h、ES 7 日、Zeek 四类来源以及根盘 70/75/80% 水位。

设计状态说明（2026-09-29）：完整单节点范围内此前开放的产品设计选择已在 [产品详细设计基线](DESIGN-BASELINE.md) 定版，包括 Windows Security 范围、UIM 质量边界、来源生命周期、过滤、任务一致性、实体归因、特征/基线/风险、SPL、导出、管理界面、生产传输、备份恢复、回放切换与验收证据。下列未勾选项继续表示代码、迁移、部署或实际验收未完成；不得因设计定版而勾选。备份接收端、证书 CA、告警 receiver 等是环境配置输入，缺失时必须 fail closed，不再作为设计开放问题。

## 当前代码切片（未等同于阶段验收）

- Raw：`internal/rawevent`、`internal/ingest`、`internal/rawindexer` 和 `internal/sink/raw.go` 已有可信单事件 envelope、PostgreSQL receipt、Kafka 确认后 202、按 UTC 接收日写 Raw ES。
- DIP/UIM：`internal/uim` 与 `internal/normalizer` 已有 Microsoft Windows Security 的一组认证/IAM/目录事件和 Zeek conn/dns/http/ssl 首批映射；合同草案位于 `contracts/uim/domain-contract.v1.yaml`。
- 索引：标准八领域和隔离索引 worker 已创建，ES 写入采用固定 UTC 日物理索引及逻辑 alias。
- 控制面：`internal/controlworker` 与 `cmd/tuba-control-worker` 已有 PostgreSQL outbox 的有界领取、聚合顺序、租约 fencing、Kafka 至少一次投递与退避；任务 worker 框架支持注册 handler，并做 queued 取消及过期租约回收。`migrations/00008_worker_leases.sql` 补充 outbox/job 字段。当前入口没有注册业务 handler，任务执行/API、审计和运行验证仍待完成。
- 来源管理：`internal/control/sources.go` 与 `internal/ingest/source_registry.go` 提供租户内来源登记/列表/撤销、API Key 摘要存储和来源级限速；COL-01 切片已加入不可变 source context、24 小时旧 key 重叠和不变更 epoch 的轮换。receipt 持久化 envelope 与 Kafka ACK 状态。
- 旧自研 Collector 代码切片（冻结新增通用采集需求）：`cmd/tuba-collector`、`internal/collector` 提供 SQLite WAL 队列、事务性入队游标、单条 Sender、Zeek JSONL 完整行读取和 equality 过滤骨架；未通过旧自研方案的完整验收，自研 Windows adapter 计划已取消，改由新 COL-05 交付 Winlogbeat 接入包。
- Collector 控制面服务端切片：迁移 `00011_collector_management.sql`、`internal/control/collectors.go`、`internal/api/collectors.go` 与 OpenAPI 已提供一次性注册 token、Agent 凭据摘要、列表/禁用、心跳、版本化配置及 ETag 读取。ZIP 客户端尚未调用这些 API，不能视为远程管理闭环。
- 限制：Zeek 独立 Filebeat 与 248 隔离链路已完成真实数据部署和首轮闭环；仍未完成过滤影子计数与永久拒绝隔离、新 Beat 来源位置/缺口适配、Winlogbeat 接入包、来源上下文发布/退休 API、端到端故障验收及通用远程部署管理。不能据此宣称 Collector 阶段已完成。

## 当前阶段与 Zeek 闭环后续顺序（2026-09-27）

宏观上仍处于 D1 数据底座实施阶段。阶段 1 已完成，A03 于 2026-09-28 定版当前 50 GiB 单节点容量边界：只允许 Zeek `conn/dns/http/ssl`，Kafka Topic 保留 24h，ES Raw/domain/Quarantine 最多保留 7 个 UTC 日分区，根盘 70/75/80% 分别为 warning/critical/停止新增写入水位。O05 的 retention guard、Prometheus、Grafana、主机与 Kafka lag metrics 已部署；邮件外发按用户要求暂缓。248 数据面自 2026-09-30 起由**产品 Launcher**（`tuba-launcher`）统一监督：六个 Zeek 组件、四个 tenant_a 组件与 api 共 11 个服务，manifest 为 `/etc/tuba/tuba-services.json`。切换前它们由 `manage_zeek_live_pipeline.py` 与 `tenant_a_chain.py` 两个 Python 监督器分别管理（见 O01 的 2b 切换记录）。source-adapter 短暂 Fetch/offset commit 错误改为有界退避重试，现场验证子进程强杀后自动恢复。2026-09-28 现场快照曾为六进程 running、source-adapter readiness=200、Prometheus 6/6 targets UP、Kafka lag=0；2026-09-29 只读复核六进程仍 running、Prometheus healthy，lag 抽样出现 TLS=1 和 network=9 的瞬时非零值，仍需观察趋势与索引新鲜度，不能沿用旧的 lag=0 作为当前状态。详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。O01/O04 的 Launcher、安装包、配置校验、依赖探针和有界批量已落地；目标主机重启恢复和 169 非管理员实际启动/停止仍未完成。Zeek 真实纵向链路已接通；这不代表阶段 2 以后或完整业务闭环完成。

1. **先处理监控发现的当前积压并完成可靠性**：248 曾部署 `start-components` 单组件恢复路径和六个 worker 的轻量监督器，异常退出按 2、4、8…秒退避，最大 60 秒，稳定运行 5 分钟后重置；`--restart start-components <component...>` 可将存活直启 worker 安全迁移。**（2026-09-30 起该轻量监督器已被产品 Launcher 取代：退避参数改为 1→30 秒，恢复路径改为 manifest-wide 的 `tuba-launcher start/stop/restart`，不再有单组件粒度；两个 Python 监督器脚本保留在磁盘上仅作回滚退路。见 O01。）**沿用原 adapter token 与 consumer group suffix，没有重置 offset。source-adapter Kafka Fetch/offset commit 增加有界退避重试。邮件接收地址 `1096429536@qq.com` 已指定，按用户要求暂缓 SMTP/Alertmanager 配置。2026-09-29 已在 21 部署按 Filebeat registry WAL/snapshot 的稳定 cursor 证明归档 stage 达到 EOF 后才回收；现场核验 144 个过期文件中先回收 36 个已确认的 conn/dns 文件，其余 108 个（约 16.8 MB）保留。248 当前六个 worker 与 6/6 scrape targets 正常；本轮部署稳定源位置/重复 receipt 兼容更新，并保持既有 consumer group 和 offsets。当前 lag 为流动值，最近抽样总量 4–8，短时主要在 DNS/network 标准索引组；需要继续观察追赶和索引新鲜度。下一步完成 COL-03、COL-07、V02 故障矩阵：隔离 Kafka 故障/恢复、目标机重启、轮转、磁盘满、Topic 重建、积压期间凭据/配置切换、真实跨 offset 重发及归档 spool 回放。不得仅凭“超过 6 小时”删除未确认归档。
2. **随后完成接入治理**：推进 COL-06 的过滤策略版本、影子计数、发布与回滚；推进 C05 的 Topic retention、ACL、分区及消费组定版。当前 Zeek 仅按四个 dataset 与最近 90 分钟归档窗口选择，尚无语义过滤。
3. **再定 UIM 质量规则**：在 N03、C02 中评审缺 SNI 的 TLS、缺 query 的 DNS、缺 host 的 HTTP 是否可以标记为 `partial`。目前原文留在 Raw，标准化结果进入 quarantine。首轮误读 gzip 的 4,295 条及旧 DLQ 423 条保留为历史证据，清理或隔离须先形成策略，不直接删除。
4. **并行准备统一部署管理**：推进 COL-08、COL-09，交付 Management Agent、组件监督、升级/回滚及主机重启恢复。**注意：Zeek 独立实例自 2026-09-30 起已由产品 Launcher 接管（统一起停/状态/日志），但"组件监督"这一项只能算部分完成**——COL-08/COL-09 真正缺的是 Management Agent、升级/回滚与主机重启恢复，这三项没有因切换而关闭；Launcher 按设计不注册 systemd，248 重启后不会自动拉起数据面，当前甚至没有受控的启动入口。
5. **后续扩展来源**：COL-05 接入 139/169 Windows Security；其后按 COL-11–13 扩展 Syslog、JumpServer、Keycloak。它们不是当前 Zeek 纵向链路的阻塞项。
6. **暂缓的长期事项**：生产传输保护、异机备份/RPO 与 X01–X05 多节点扩展保留在既有任务中；其中容量与恢复限制仍须在 A03/V07 给出单节点决策，不以多节点部署作为当前前提。
7. **API 运行时跟进（OpenAPI 已定版）**：依 OpenAPI 的 `planned` 标记实现 release/job/quality/entity/risk/query 路由；为已存在的来源/Collector 列表补 cursor/limit，为来源创建补幂等键。直到这些 handler 具备真实授权与持久化验收前，不能将规划合同当作运行能力。

## 使用规则

- 按阶段依赖实施；同一阶段内部仅在合同已明确时并行。
- 每个任务关闭需记录代码/合同路径、迁移方式和验收证据；有阻塞需记录原因。
- “验证/演练”条目只有记录实际执行证据后才能关闭；设计修改不代表演练完成。
- P0 为完整数据与安全闭环的前置条件；P1 为本轮完整产品必交付；P2 为未来多节点，非本轮阻塞项。
- 开发归属：G=Go，P=Python，W=Web，D=数据库/存储，O=部署运维；是责任类型，不代表已分配人员。

## 阶段 0：基线与环境盘点（P0）

- [x] A01（G/D）整理现有代码与目标差异表，逐一标记复用、改造、新增、停用。产物：[IMPLEMENTATION-GAP-MATRIX.md](IMPLEMENTATION-GAP-MATRIX.md)；完成条件：每个目标服务有代码落点。矩阵同时标明现有代码切片不等同于阶段验收。
- [x] A02（O）只读盘点 248 的 CPU/内存/磁盘、端口、服务版本、数据目录、Topic、索引、进程、身份依赖。产物：[ENVIRONMENT-248-REPORT.md](ENVIRONMENT-248-REPORT.md)；完成条件：区分配置存在与运行正常，不记录密钥。报告记录了 Topic/消费者与合同的差异，未变更环境。
- [x] A03（O）确认采集范围、日量/峰值、平均事件大小、Raw 保留及备份目的地，形成单节点容量预算。2026-09-28 依据 21/248 实测关闭当前 50 GiB 开发单节点边界：仅接入 Zeek `conn/dns/http/ssl`；每路 Filebeat queue 256 MB；解压 stage 6h；所有 Kafka Topic 24h；ES Raw/domain/Quarantine 最多 7 个 UTC 日分区；根盘 70/75/80% 为 warning/critical/停止新增写入水位。Kafka 约 3.8 GiB＋ES 1.4 GiB/日的保守预算下，7 日总量约 13.6 GiB，低于保留 20% 根盘空闲后的约 18.2 GiB TUBA 预算。后续来源和分析索引启用前必须重新测量。当前无异机备份，磁盘/整机损失无 RPO/RTO 承诺；短时故障仅在磁盘完好且未超过 Kafka/队列边界时目标 RPO=0。完整证据见 [CAPACITY-OBSERVATION-20260927.md](CAPACITY-OBSERVATION-20260927.md)。实际 retention、水位告警及清理 job 部署归 O05。
- [x] A04（G/D）为 Zeek 验收链路分配隔离 namespace、generation 和消费组；记录旧入口兼容/退出策略。已部署 namespace=`zeek_validation_20260927_001`、generation=`g1`、独立 SCRAM/ACL broker（248:29292）、四个 source context/Topic、Raw/Event/Quarantine Topic 及 r2 消费组；未修改 248 原有 9192/9193 broker，也未接管 21 原有 Filebeat。旧 profile 保留为历史记录。
- [x] A05（G/O）梳理现有产品状态、DEVELOPMENT、DEPLOYMENT 等细分文档，保留旧验收证据。完成条件：目标要求与历史状态清楚区分，所有文档不与目标基线冲突。已将仓库 README 改为当前代码切片状态，建立文档权威顺序并在索引中列出 A01/A02/A03/A04 产物；修正 DEVELOPMENT 的 M5/OIDC 旧状态描述；明确 DEPLOYMENT 是 Helm 扩展参考且不得作为当前单节点安装方式。M1–M5 文件保留为历史证据。

## 阶段 1：合同、数据模型与基础迁移（P0；依赖 A01、A04）

- [x] C01（G）新增 raw/source envelope JSON Schema 与样例，固定可信上下文、大小限制、source_position、raw_event_id、时间及多记录规则。合同：`contracts/events/raw/1/schema.json`、`contracts/events/raw/1/contract.md`、`contracts/examples/raw.valid.json`；`internal/rawevent` 对齐租户 ID、来源上下文格式、长度、单条 JSON object、1 MiB payload、接收时间和稳定位置 ID 的运行时约束。来源注册、授权及重置联动由 I01/C06 跟踪，不属于合同定义缺口。
- [x] C02（G）制定公共 UIM＋八领域合同、qualified/partial/隔离原因、usable_for、路由注册表；给出缺字段/类型/时间/未知来源样例。`domain-catalog.v1.yaml`/`domain-contract.v1.yaml` 定义八领域必需字段、质量和路由责任；standard-event v1 schema 覆盖共享字段和领域字段类型。机器用例 `validation-cases.v1.json` 驱动 `internal/uim` 行为验收，覆盖 Zeek 缺字段/错误类型/无效时间、未知来源及 Windows 失败认证；`scripts/validate_contracts.py` 验证 8 个 schema、canonical 示例及用例结构。证据：`python scripts/validate_contracts.py` 通过（8 schemas、8 examples、5 UIM cases）；`go test ./internal/uim` 通过。用例只承诺第一批 Windows/Zeek parser coverage，不能解读为八领域来源映射均已交付。
- [x] C03（G）制定 event/entity/attribution/relation/feature/anomaly/risk ID 编码规范和版本，覆盖来源重置、弱身份、作用域和多角色。实现：`contracts/ids.md`；当前代码只生成 raw_event_id。
- [x] C04（G/P）扩展结果合同，包含 object_type、revision、operation/retracted、generation、输入引用及稳定日期键；保留旧异常合同的迁移规则。新增不可变 [analysis-result v2 schema](../contracts/events/analysis-result/2/schema.json)、样例及 v1→v2 迁移约束：generation/input_refs 缺失时使用显式 legacy 值/空集合；缺稳定窗口时间的旧消息隔离，不以处理时间补造。Topic 目录保留 v1 只读兼容并指定 v2 新写入。证据：合同校验通过；producer/sink 迁移实现归属 F06/F07，不包含在合同定版中。
- [x] C05（G/P）定义各 Topic、物理命名映射、key、consumer group、ACL、分区、保留和事务配置。`contracts/events/topics.v1.json` 为 9 类逻辑 Topic 固定 `physical_name`；`scripts/validate_contracts.py` 验证映射唯一、单节点分区/副本、key、消息上限、retention、ACK/提交、source ACL、group hash 和结果分流。validation profile 显式设为 1 partition/1 replica、2 MiB、24h、manual commit、at-least-once、无 Kafka transactions；生产 retention 目标 Raw/domain 7d、Quarantine/DLQ 14d 仍 provisional，待 A03。248 上当前 21 个 TUBA Topic（涵盖已运行的来源接入、Raw、八领域、隔离与 DLQ 路径）已对账；entity/analysis 阶段 Topic 仍为合同定义，待相应 worker 交付时创建。已部署并验证 6 个逐服务 SCRAM principal，source Filebeat 凭证各限于一个 literal source Topic；处理服务按职责获 literal topic/group ACL，default-deny 生效。6 个数据面服务重启后均正常，15/15 消费组有 active member；验证时在途 lag 为 4，认证/授权错误为 0。旧共享 worker 的 namespace 前缀 ACL 已撤销，只保留旧 validation Topic/group 的 literal READ 权限；placeholder Filebeat 的旧 context 写 ACL 已撤销，历史 topic/offset 保留。完整实际目录和 ACL 计数见 [ENVIRONMENT-248-REPORT.md](ENVIRONMENT-248-REPORT.md)。验收：`python scripts/validate_contracts.py`、`go test ./internal/sourceadapter ./internal/uim` 通过。
- [x] C06（D）设计 sources/assets/releases/jobs/leases/inbox/outbox/state/checkpoint/result_revision 及实体时态表 migrations；明确复用现有表和 tenant 约束。新增 [DATA-MODEL-BASELINE.md](DATA-MODEL-BASELINE.md) 对照 00007–00012 表责任、作用域/FK、不可变来源上下文、fencing 和结果修订；release manifest 内嵌 asset index、不单独建资产表的理由及 API/worker 未交付范围已记录。新增 00012 约束 source context 的授权元组必须与父 source instance 一致。验收：隔离 PostgreSQL 18.3 WASM（PGlite 0.5.8）启用 pgcrypto 后，全迁移 00001–00012 正向执行及逆序回滚通过；00012 对合法上下文放行、对 namespace 不匹配拒绝。阶段 2 另在 O02 对 PostgreSQL 14.23 完成迁移运行 preflight。证据见 [DATA-MODEL-BASELINE.md](DATA-MODEL-BASELINE.md) 与 O02 记录；API/worker 集成属于后续阶段。
- [x] C07（D）生成 ES 公共/领域模板、确定性时间索引策略、alias/generation 清单、retention 清理规则；处理旧 Data Stream 同名冲突。已新增从 canonical JSON Schema 确定性生成的 Raw、Quarantine、UIM common＋八领域 component/index templates（1 primary/0 replica）和有保护条件的 retention 配置；`python scripts/generate_es_templates.py` 校验生成结果无漂移。索引按稳定事件/采集 UTC 日期进入 `tuba-v1-*` 物理索引，indexer 拥有逻辑 alias。2026-09-27 对 248 ES 8.19.0 做只读核查，`GET /_data_stream` 返回空列表；authentication alias `logs-ueba.authentication-n06_20260926_001` 指向存在且有 1 条文档的物理索引，因此旧同名 Data Stream 不存在，迁移条件不适用，也没有覆盖或删除任何数据。retention 清理保护规则与 validation 模板已校验；生产保留天数仍须 A03 按容量/RPO 定案。证据见 [环境报告](ENVIRONMENT-248-REPORT.md)。
- [x] C08（G/W）补齐来源、发布、任务、质量、实体、风险和查询的 OpenAPI；固定权限、错误码、分页、幂等与异步状态。OpenAPI 已补 release register/validate/activate、job lifecycle、quarantine quality、entity/risk list、bounded query 契约；注明 permission extensions、cursor/limit、Idempotency-Key、异步 202、明确错误场景和禁止 raw DSL，并将未实现操作标记 planned。验收快照：YAML 可解析；39 paths/44 operations 的 operationId 唯一；111 个 internal/external refs 均解析。契约完成不代表 planned 路由已实现；已实现 sources/collectors 中待补的分页/幂等行为转入对应后续实现任务。
- [x] C09（G/P）定义发布包 manifest 与依赖检查：DIP/UIM/路由/Mapping/实体规则/Data Model/分析资产哈希。独立 manifest schema 与迁移/不可变规则定义七类资产、SHA-256、contract/generation compatibility、bundle 内相对路径、唯一 kind/ID、存在依赖和无环图；`validate_release_bundle.py` 对实际文件流式验哈希并输出 canonical manifest 摘要。证据：合同检查通过；临时七文件 bundle 验证成功，篡改文件被拒。发布状态 API 属 C08/后续控制面实现；Beat 制品 manifest 仍独立。

阶段出口：合同可独立评审；所有 ID、时间、租户、版本、物理路由有唯一生成责任；C05 Topic/ACL/group 有运行验收证据；C06 migrations 完成全序列 up/down；C07 ES 模板校验且不存在旧同名 Data Stream。以上出口已于 2026-09-27 达成；A03 于 2026-09-28 补齐当前 50 GiB 单节点容量和恢复限制。阶段 2 可部署 retention guard，但异机备份仍没有恢复承诺。

## 阶段 2：单节点运行基础（P0；依赖 A02、A03、C05–C07）

- [ ] O01（O）建立版本化安装目录、非 root 服务账号、权限、配置文件及密钥注入；统一由产品 Launcher/CLI 管理进程启动、停止、重启、状态与日志，不注册 systemd 或 Windows Service。进行中（2026-09-28）：新增 `cmd/tuba-launcher` 与 `internal/launcher`，统一管理服务清单、PID/状态、私有日志、停止请求、优雅退出及失败指数退避重启；服务配置用精确 `${ENV_NAME}` 引用注入，子进程只继承基础 OS 环境和清单显式字段。Unix 环境文件要求 0600；Windows 启动时核查 ACL，并限制 Launcher state/log 目录。新增双平台服务清单、`scripts/package_tuba.ps1`、Linux/Windows 安装器；Linux 创建不可登录 `tuba` 账号并使用 `/opt/tuba/releases/<version>`＋`current`，Windows 使用 Program Files 版本目录＋current junction 和专用运行账号。Unix 服务接收 SIGTERM；Windows 服务使用隐藏的独立控制台进程组接收 Ctrl-Break，10 秒后由 Launcher 的 `WaitDelay` 强制收尾。验证：Windows 本机 start/status/stop 与 Launcher 密钥/重启测试通过；新增 Ctrl-Break 子进程回环验证，清理处理器执行后正常退出，连续 3 次通过；Linux 安装包在一次性 Ubuntu 24.04 容器（init 负责回收子进程、无端口映射或宿主挂载）中通过校验和安装，`tuba` 账号权限/目录 mode 正确，非 root start/status/stop smoke 通过；Alpine v3.23 rootfs 中另验收版本切换与升级后生命周期。当前 `go test ./...`、双平台包 SHA-256 与示例 manifest 环境引用校验通过。未完成：Ubuntu 裸机目标安装、Windows 专用账号真实安装、Windows/Linux 升级切换在目标主机及生产权限验收。故不勾选。
  - **2026-09-30 复核：阻塞项未变，但有一处需要确认的不一致。** O01 的交付描述是 Unix 使用 `/opt/tuba/releases/<version>`＋`current`、创建不可登录 `tuba` 账号并由 Launcher 以该账号运行；而 248 上的实际布局是本会话早前实测到的**扁平布局**——组件运行于 `/opt/tuba/bin/` 与 `/opt/tuba/collector-live/pipeline/bin/`，`/opt/tuba/releases` 仅 52 KB（版本化安装树会是百 MB 量级）。也就是说**安装器只在一次性容器里验收过，目标主机上是另一套手工部署**，两者并未对齐。这条的方向已由下一条确认：安装器对齐现实，不重装 248。**（本段写于 248 直连一度不通时——判断来自经 21 的间接读数。2026-09-30 晚已恢复 248 直连并完成 2b 切换，扁平布局经直连实测确认，见下方 248 实测条目。）**
  - **方向已定（2026-09-30）：安装器对齐现实，不重装 248。** 依据是 `TARGET-ARCHITECTURE.md` 11.1 已写明的「现有 248 路径可通过配置接入，升级不搬迁数据库目录」——设计要求本就是安装器适配既有环境。据此把安装器的**职责边界**显式化并落到代码：
    - **TUBA 自有**（安装器创建/版本化/回滚）：`/opt/tuba/releases/<version>`＋`current`/`previous`、不可登录 `tuba` 账号、`/etc/tuba`、`/var/lib/tuba`、`/var/log/tuba`。
    - **领养而非安装**（只验证、不改动）：PostgreSQL/Kafka/Elasticsearch/Keycloak 实例本身。
    - 新增只读预检 `scripts/check_tuba_prerequisites.sh`：**不下发任何 DDL/DML、不创建 Topic、不改集群设置**；三类情况 fail-closed——`public` 含非 TUBA 表（共用库）、运行角色即 schema owner（248 现状）、单数据节点 ES 仍用默认 85%/90% 水位（后者只告警）。前两类可按名确认（`TUBA_ADOPT_SHARED_DATABASE` / `TUBA_ALLOW_RUNTIME_OWNER`）。
    - `scripts/provision_postgres_runtime_role.sh` 原本会对**共用库**执行 `REVOKE CONNECT ON DATABASE ... FROM PUBLIC`、`REVOKE CREATE ON SCHEMA public FROM PUBLIC`——在 248 这种库上等于让另一个产品掉线。现改为默认拒绝，需显式确认；判定逻辑抽到 `scripts/lib/tuba_pg_adoption.sh`，与预检**共用同一份**，避免"预检放行、供给拒绝"的自相矛盾。
    - 接入：`initialize_tuba_single_node.sh` 把预检作为第 1/6 步（新增 `--check-dependencies-only`），并有编配测试断言**预检失败时后续步骤一个都不跑**；`make shell-check` 纳入 `check`。
    - 验证：`scripts/test_check_tuba_prerequisites.sh` 用 fake `psql`/`curl` 驱动，**并记录收到的每一条 SQL**以证明预检只读——当初用退出码断言时，"预检偷偷下 DDL"这个变异**没被抓到**（`psql_scalar` 丢弃 stderr 且 `head` 吃掉退出码），改为断言 SQL 日志后才成立。共享库、schema owner、退出码丢失、`--check-dependencies-only` 失守等变异逐一验证被捕获。**仍未关闭**：预检与守卫都只在 fake 上验过，未在 248 真实库上跑；Ubuntu 裸机安装与目标机验收依旧未做。
  - **2026-09-30 248 实测：Launcher 已接管全部 11 个数据面服务（2b 切换完成）。** 248 的实际数据面是 **11 个服务**：`api` + 6 个 zeek 链（ingest、source-adapter、raw-indexer、normalizer、quarantine-indexer、standard-indexer）+ 4 个 tenant_a 链（raw-indexer、normalizer、quarantine-indexer、standard-indexer）；此前文档里的"9 服务"/"10 服务"是包内示例清单，不是 248 现状。切换前 `api` 由人工从 `/root` 启动（非 systemd，`/opt/tuba/start.sh` 是 M1 遗留、已不适用），其余 10 个由 10 个 python 监督器各自 `--supervise-binary` 看护。
    - **清单不是抄配置文件，而是从运行中的进程推导**（读 `/proc/<pid>/environ`）：248 的真实配置是"API 环境 ∪ 监督器覆盖 ∪ Kafka 密钥文件"在启动时拼出来的，磁盘上没有任何一份文件等于运行态。共享密钥（`DATABASE_URL`/`ES_API_KEY`/`TUBA_INGEST_API_KEY`/`SOURCE_ADAPTER_TOKEN`）按**值**折叠成单一变量，避免逐个服务输出导致漂移、静默打断 ingest↔adapter 握手。
    - **切换中暴露的真实缺陷：白名单式环境捕获会静默丢变量。** 生成器只搬运密钥清单与一份"字面量"清单，而 `ES_URL` 不在任何一份里，于是被整条扔掉；结果是 11 个服务里 7 个立即 `exit 1`，日志只有 `ES_URL and ES_API_KEY are required`（`ES_API_KEY` 在、`ES_URL` 没了）。补齐后 11/11 running。**教训**：环境捕获不该用白名单——漏一个变量不会报错，只会在运行期以别处的报错形式浮现。生成器已改为"照抄每个服务实际拥有的环境"，只丢三类：密钥改 `${...}` 引用、监听项、以及一个小而显式的 shell 环境噪声清单（`PWD`/`SHLVL`/`TERM` 等——照抄会把 `PWD=/root` 交给在 `/opt/tuba` 运行的服务）。另有一条同源风险已排查：Launcher 读环境文件时做 `TrimSpace`，值含首尾空白会被改写；实测 248 环境无此类值。
    - **新增的 fail-closed 守卫，以及它自己被抓出的两个漏洞。** 守卫规则："任何在某个运行服务里出现过、却到达不了任何服务的变量，拒绝写盘"。**变异测试抓出守卫本身两次失明**：① 守卫原先用跳过清单（`AMBIENT_DENY`）来豁免，于是把 `ES_URL` 加进跳过清单就同时把守卫弄瞎了——正是原缺陷的形状；改为一份**独立的第二份字面量**（`MAY_DROP`），只改一边就不一致、工具拒绝写盘。② 守卫原先复用拷贝循环的变量名正则，把它改窄就同时从守卫视野里藏掉该变量；改为守卫自带一个**不同的**名字判定。两次变异都必须能被触发，这是它还算个守卫的前提。
    - **验收证据（不是"进程起来了"而已）**：① 消费组集合切换前后**完全一致**（33 → 33，无一消失、无一新增）→ 偏移连续，没有新建消费组从 earliest 重放；② ES 各域文档数持续增长（90 秒内 raw-zeek +1005、uim-network +403、dns +118，tenant_a 各域同步上涨）；③ 稳定观察期内 11 个服务 `restarts=0`；④ `api` 监听 `127.0.0.1:8788`，`/metrics` 200、`/api/v1/*` 401（需鉴权，符合预期）。
    - **回滚路径**：`tuba-launcher stop --manifest /etc/tuba/tuba-services.json` → `python3 /opt/tuba/collector-live/manage_zeek_live_pipeline.py start`、`python3 /opt/tuba/collector-live/tenant_a_chain.py start` → 重启 api。监督器的 `start` 从**运行中 api 的** `/proc` 环境取基座，所以必须先起 api 再起监督器。
    - **顺带证实旧监督器的 `stop` 不完整**：`stop` 只回收了 6 个 zeek 子进程里的 2 个，留下 4 个孤儿仍在消费 Kafka（其中一个忽略 SIGTERM、需 SIGKILL）。两套消费者同时在同一消费组里会让索引重复写入，这也是替换监督器的直接理由。
    - **接管后遗留（2026-09-30 未完成，按阻塞原因列出）**：
      1. **开机恢复仍缺——当前最大的可用性缺口**：Launcher 按设计不注册 systemd，248 重启后不会自动拉起数据面。缺的不是机制而是"谁在启动后调用 Launcher"：受控开机任务或 Management Agent（COL-08/COL-09）。注意这不代表回退可接受——回退到监督器同样没有开机恢复。
      2. **非 root 转换未做**：`tuba` 账号已建（uid=967、nologin），但 `/opt/tuba/collector-live` 是 `0700 root`，Launcher 目前以 root 运行。转换需搬迁二进制/改权限并重新验收权限边界，本轮未做。
      3. **Launcher 二进制不在包/校验链里**：248 保持扁平布局（`/opt/tuba/bin`、`collector-live/pipeline/bin`）是**已定的方向**（安装器对齐现实，不重装 248），所以"未收敛到 `releases/<version>`＋`current`"不是缺陷。真正的缺口是 `/opt/tuba/bin/tuba-launcher` 是本次手工 `install` 上去的，不来自任何包，没有 SHA-256 sidecar 与包审计链。也就是说 248 现在跑的是"Launcher 管进程、但 Launcher 本身不是受管制品"的中间态。
      4. **没有单服务回滚粒度**：原计划的 2b-3"单服务试切"被用户的"接管全部"决定取代。Launcher 的 `start`/`stop` 是 manifest-wide，`restart` 也是全量重启；要单独回滚一个服务只能退回旧监督器（且回退前须按 RUNBOOK 核对无孤儿进程，否则两套消费者同组会重复写索引）。
      5. **`/opt/tuba/start.sh` 仍是地雷**：它会 source 语义已变的 `/etc/tuba/tuba.env`，起一个缺 `ES_URL` 的 api。尚未废止或加守卫。
      6. **预检与供给守卫仍未在 248 真实库上跑过**：仍只在 fake `psql`/`curl` 上验过（见上一条）。
      7. **监控栈不在清单内**：prometheus/grafana/node_exporter/kafka_exporter 独立于 Launcher 运行，未纳入统一管理，重启后同样不会自动恢复——这是第 1 条的另一个实例。
      8. **未做持续稳定性窗口**：验收只有分钟级观察（11/11 running、`restarts=0`、33 个消费组不变、各域文档数上涨）；跨天趋势、跨 offset 重放对账与故障注入（COL-07/V02）仍未做。
      9. ~~**生成清单的工具不在版本控制里**~~ **（已收编）**：`gen248.py`（从 `/proc` 推导 manifest＋env 的生成器，即本次修复白名单缺陷的那个文件）原先位于 `.runtime/`、被 `.gitignore` 排除，没有 review、历史或测试。现已移入版本控制为 `scripts/gen248.py`（内容不变），两条守卫的变异用例已固化为 `scripts/test_gen248.py`（`python scripts/test_gen248.py`，7 例）：共享密钥值漂移拒绝写盘及一致值只落地一次的正例；"live 变量到不了任何服务则拒绝写盘"守卫——含加宽 `AMBIENT_DENY` 而不动 `MAY_DROP` 的变异（模拟 `ES_URL` 被静默丢弃）必须触发拒绝、`MAY_DROP` 内变量（`PWD`/`HOME`）被丢弃不触发、以及守卫与拷贝循环的名字正则必须不同、`MAY_DROP` 必须是独立第二份拷贝的结构性断言；外加两服务最小 fixture 的 happy-path（wildcard 监听进 `dropped_listeners`、每服务密钥变 `${TUBA_<NAME>_...}`、共享密钥变 `${VAR}` 引用）。
    - **同批结转的待办**（不在本次切换范围内，但切换后仍未关闭）：COL-07/V02 破坏性故障注入子集——阻塞于磁盘扩容，且用户已决定本轮不做目标机重启测试；Adapter/DLQ 端到端验收测试——用户推迟到后续会话。
    - **一条需要记住的语义变更（不是待办，是现状）**：`/etc/tuba/tuba.env` 已从"api 的环境文件"变成"Launcher 的密钥文件"，只含密钥。已核对 `backup_tuba_to_offsite.sh` 自带 `ES_URL` 默认值、不 source 该文件，全机再无其他消费方，故无回归；但任何**新**脚本都不应再把它当作完整服务环境来 source。
- O01 Linux 包核对补充（2026-09-28）：修复 PowerShell `.sha256` sidecar 的 CRLF 文件名问题，包脚本改为无换行 ASCII 输出，Linux 安装器使用 `sha256sum -c`。WSL Alpine 实测 Linux 包 checksum 返回 `OK`，`tar -tzf` 可完整读包；这验证包格式/校验链，不替代在 Ubuntu 目标主机执行安装器。
- O01 Linux 安装补充（2026-09-28）：在隔离 Alpine v3.23 rootfs 中执行 Linux 安装器并从 RC6 升级到 RC7，`current` 切换且保留旧版本，升级后 `tuba` 非 root smoke service 的 status/stop 通过。随后在一次性 Ubuntu 24.04 容器中执行当前 Linux 包安装器，SHA-256 通过；创建不可登录 `tuba` 用户，验证 release/config/state/log 权限，使用 init 作为 PID 1 后以 `tuba` 非 root 完成 start/status/stop。一次无 init 的容器运行出现 PID 1 不回收的僵尸进程，未计为通过；加入 init 后 stop 在 10 秒内成功。两次容器均无宿主数据挂载，未部署到 248。Ubuntu 裸机目标安装和 Windows 专用账号安装仍未验收，O01 保持未勾选。
- O01 Windows 升级/回滚补充（2026-09-28）：Windows 安装器现在先完成新 release ACL 和受保护配置 ACL，再用新 Launcher 的 `validate` 子命令检查升级 manifest、可执行文件、环境引用及 Windows 私密文件权限；只有通过后才请求旧 Launcher 停止。current junction 切换保留上一版为 `Program Files\TUBA\previous`；切换失败时恢复旧链接并尝试重新启动旧版本。新增 `scripts/rollback_tuba_windows.ps1`，通过受控交换 current/previous junction 执行回滚，并始终保留版本目录。`scripts/test_tuba_release_windows.ps1` 在唯一临时目录真实创建 Windows junction，覆盖首次激活、升级、回滚交换、无效 release 失败关闭和链接保留；通过。`go test ./...`、PowerShell 语法校验和双平台包构建/allowlist 审计通过；当前 RC11 SHA-256：Windows `487eab6e1092dacaf69da7d0e970b01f8a0a2316550a8deabedf12eb1032df2b`，Linux `6a617a387763f5e2d33cf2f9b786720d58e52790fde5cf09a17736b4e069c6c6`。隔离包位于 `dist/stage2-rc11/`。以上不是专用账号安装或完整 TUBA 服务目标机验收；O01 仍未关闭。
- O01 Linux 安装升级/回滚补充（2026-09-28）：`install_tuba_linux.sh` 现会拒绝 UID 0、属于 `root/sudo/wheel` 组或 shell 非 `nologin` 的既有 `tuba` 账号；升级前完成当前 manifest/environment 验证，再停止 Launcher。新增 `scripts/lib/tuba_release_linux.sh` 统一验证 release link 必须解析在 `releases/` 内，升级原子切换 `current` 并保留上一版 `previous`；新增 `scripts/rollback_tuba_linux.sh`，先有序停止、交换链接、再启动回退版本，失败时尝试恢复原链接。`scripts/verify_tuba_linux_install.py` 在网络隔离、只读 workspace mount、带 init 的一次性 Ubuntu 24.04 容器内用 RC9 首装、RC10 升级并执行真实 Linux Launcher validate/start/status/logs/stop 与独立 rollback 脚本；检查非 root/nologin 账号、state 和环境文件权限、current/previous 目标及 sidecar SHA-256，完整通过。此为隔离容器验收，不替代 Ubuntu 裸机安装或目标机重启恢复；O01 仍未关闭。
- O01/O03 RC12 双平台包及升级验收（2026-09-28）：基于当前代码生成 `dist/stage2-rc12/` Windows/Linux amd64 包；两包各通过 17 项文件 allowlist、manifest 环境变量/密钥占位符审计和 SHA-256 生成。哈希：Windows `ee74556dd62c40cc77f2ba2102e5822438764aa386a7c9caec66f9e7508c1c87`，Linux `28341e5c74df4b36ba65bdc38d2b328c983a87cd6d86099b3b7cadbd0d80e1fb`。随后在无网络、只读工作区挂载、带 init 的一次性 Ubuntu 24.04 容器中由 RC11 升级到 RC12；SHA-256、manifest validate、专用非 root/nologin 用户、环境/state 权限、current/previous 切换、Launcher start/status/logs/stop 和独立 rollback 均通过，容器未连接产品依赖或 248。该运行证据覆盖 Linux 包的最新监听配置代码，不替代裸机目标验收；Windows 本轮完成构建/allowlist 审计，未执行专用账号安装。O01/O03 仍未关闭。
- O01/O04 Windows RC12 包 manifest 验收（2026-09-28）：新增 `scripts/verify_tuba_windows_package.ps1`，核对 zip SHA-256 与归档路径后，使用归档内的 Windows Launcher 对完整 9 服务 manifest 执行真实 `validate`；服务可执行文件、环境变量引用和受限环境文件 ACL 均通过。该验收不启动服务、不写 Program Files/ProgramData，也不创建 Windows 账号；它补充包内配置验证，不替代专用账号真实安装和进程生命周期验收。临时目录权限收紧后的清理路径亦已修复并复验通过。
- O04 Launcher listener preflight（2026-09-28）：`tuba-launcher validate` 复用 `internal/config.ValidateListenerAddress`，检查 manifest 中显式配置的 `HTTP_LISTEN`、`API_LISTEN`、`METRICS_LISTEN` 和所有 `*_METRICS_LISTEN` 及 `TUBA_ALLOW_NON_LOOPBACK_LISTEN`，使错误监听配置在版本激活/停止现有进程前失败。测试覆盖 API/ingest/worker/Python metrics 的默认拒绝 wildcard/外部 IP、显式 opt-in 接受、拒绝 DNS hostname 与大小写/数字等非精确布尔值。全量 `go test ./...`、Helm 渲染通过；Windows/Linux RC13 包完成 allowlist 审计，Windows 包内 Launcher 对完整 9 服务 manifest 与 HTTP wildcard opt-in 正反例 validate 通过；Linux 在一次性 Ubuntu 24.04 隔离容器从 RC12 升级至 RC13，manifest validate、非 root 账号/权限、current/previous、生命周期及 rollback 均通过。该验收不运行真实 TUBA 依赖服务，不替代目标机或 Launcher 管理真实产品服务的运行验收。
- O01/O03/O04 RC13 双平台包（2026-09-28）：`dist/stage2-rc13/` 包 SHA-256 为 Windows `e12e038d7e08eb434ee79804577f38ea43c065c068fbd5cad21059f73080c928`、Linux `270ba13d86185be4b43ac122b4bdf80d17909a84a8814683de26715a33397710`；包内代码包含 Launcher listener preflight。上述隔离升级/manifest 验收均使用这组候选包；尚未在 Windows 专用账号或裸机目标安装。
- O01 Linux installer 归档校验收口（2026-09-28）：审计发现此前 `sha256sum -c` 会照 sidecar 指定的文件名验证，攻击/误配置的 sidecar 可验证另一个文件而实际解压目标归档。`install_tuba_linux.sh` 现严格要求 sidecar 恰好一行、64 位十六进制哈希、归档文件名精确匹配，并直接计算归档本身哈希后才继续；归档内容还只允许常规文件和目录，拒绝符号链接、硬链接及特殊文件。扩展 `scripts/verify_tuba_linux_install.py`：一次性 Ubuntu 24.04 网络隔离容器验证“错误 sidecar 指向其他文件”和“哈希正确但含指向 `/etc` 的符号链接”均在创建 release/服务账号前被拒；随后 RC12→RC13 安装升级、权限、Launcher 生命周期和独立回滚全通过。未连接产品服务或 248。
- O01 Windows link 回归（2026-09-28）：执行 `scripts/test_tuba_release_windows.ps1`，在唯一临时目录创建真实 junction，首次激活、升级、回滚、非法 release 拒绝和失败时保留链接均通过；仍不等价 Windows 专用服务账号下真实安装/升级。
- O04 Launcher CLI 运行验收补充（2026-09-28）：新增 `scripts/verify_launcher_cli.py`，临时构建当前平台 `tuba-launcher` 与仅用于本次验收的可处理中断 helper，写入唯一系统临时目录和 manifest，不连接 Kafka/PostgreSQL/ES 或 248。通过实际 Launcher CLI 验证 `validate`（有效 manifest 放行、缺失环境密钥引用拒绝）、`start`、`status`、`logs`、`restart`、`stop`，并核对两次 helper 清理标记、停止后状态和日志；完整脚本通过。验收发现并修正 clean `runner_pid=0` 被误报为 stale state 的状态描述，真实失效 PID 仍显示 stale。该证据从 CLI 端覆盖 supervisor 的 detached process 启动和完整控制流，但服务仍是测试 helper，不是 TUBA ingest/worker；O04 的目标主机恢复验证与完整产品服务 manifest 验收仍待完成。
- [ ] O02（O）建立单 Kafka/PG/ES/Keycloak 安装和初始化流程；Topic、迁移、模板重复执行安全；默认单分区、单副本，ES 1 主分片/0 副本。仍未关闭：2026-09-28 已完成一次性完整依赖栈初始化与重复执行验收（见 O02 补充）；不等同 Ubuntu 裸机/Windows 专用账号目标安装，也不解除 A03 门槛。
  - PostgreSQL：`scripts/apply_postgres_migrations.sh` 使用 SHA-256、advisory lock 和每迁移事务；新增 `scripts/provision_postgres_runtime_role.sh` 为应用配置 DML-only `tuba_runtime`，迁移专用 `DATABASE_MIGRATION_URL`，以及 `scripts/verify_postgres_migrations.ps1`。在一次性 PostgreSQL 18.6 项目通过 12 个迁移首次/重复执行、双进程并发、checksum drift 失败关闭、故障迁移的 DDL 与账本记录回滚、运行账号重复 provision、新表 DML 成功、DDL 和迁移账本访问拒绝。复核发现组合验收最初因 PowerShell 插值错误，将 runtime role provision 到默认 `tuba` 数据库而不是临时 runtime 数据库；已修正 `$runtimeDatabaseName` 插值，之后整套验收通过。脚本要求显式容器名并自动删除临时数据库，不连接产品 PG。14.23 容器无端口映射、无产品数据挂载，旧迁移兼容验收证据保留；该 preflight 不代表已批准对 248 现存 schema 执行迁移或生产升级。
  - Elasticsearch：本机 ES 8.19.22 已接受 canonical 11 个 component templates、10 个 index templates 和 anomaly mapping 的安装及重复 PUT；模板 GET 与 anomaly mapping 读回核验通过。没有安装 ILM 删除策略，正式保留仍待 A03。
  - Kafka：本机 Kafka 4.3.1 用临时 namespace 应用并读回验证 15 个 bounded validation Topic，分区/副本均为 1，配置符合 validation retention、min ISR 与消息上限合同；临时 Topic 已删除。另在独立临时容器内以 Kafka 4.3.1 完成 default-deny 安全 KRaft broker 验收：SCRAM 用户无 ACL 写临时 Topic 被拒，broker 重启后仍拒绝，Topic/凭据保持。新增 `cmd/tuba-kafka-security-admin`，按 bounded profile 输出六个数据面 principal 与 literal Topic/group ACL 计划；实现 SCRAM-SHA-512 凭据 upsert 和 ACL add/readback，密码从受保护环境变量读取且不回显。该 CLI 已在一次性 Kafka 4.3.1 KRaft authorizer broker 上真实 apply 并重复 reconcile：6 个 SCRAM principal、61 个 literal ACL 均成功写入/读回；同时修复并验证了 Kafka 4.3 `DescribeAcls` 请求必须显式指定 pattern type 的兼容问题。PG14.23、Kafka ACL broker 及其临时卷/镜像均为隔离资源，已清理，不触及 248。Linux/Windows 安装包均包含该 CLI 且 SHA-256 核对通过。Keycloak 的一次性 PostgreSQL 持久化和 realm 更新已隔离验收；目标主机安装及正式环境验收仍未完成。
  - Keycloak：增加 PostgreSQL Compose overlay 与幂等数据库引导脚本，使用独立 `keycloak` 库/角色；专用角色通过读回验证不具备 SUPERUSER、CREATEDB、CREATEROLE、REPLICATION、BYPASSRLS。2026-09-28 在独立 Compose project（PG 18.6、Keycloak 26.7.4，独立卷和 15432/18181 端口）完成首次部署、重复执行 DB 初始化、`tuba` realm 首次导入、OIDC discovery、Keycloak 重启后再次 discovery 和数据库 realm 记录读回；未访问正在运行的 `product-postgres-1`。验收后删除专用容器、网络及数据库卷，并确认 `product` Kafka/PG/ES 仍 healthy。realm 更新流程已写入部署说明：备份专用库、版本化记录 Admin REST/kcadm 操作、读回校验；realm 文件重启导入不作为升级。Keycloak Compose 已改为敏感变量缺失时 fail closed；realm 变更操作与隔离运行验收见下方 O02 补充。完整依赖栈的隔离初始化见下方 O02 补充；O02 仍需目标主机安装验收，且受 A03 容量/恢复门槛限制。
  - Docker Desktop 本机服务已启动以完成上述隔离验证；未部署或修改 248。A03 容量/保留/异机备份与 RPO/RTO 未定，生产 profile 仍不能定版。
- O02 完整隔离单节点初始化补充（2026-09-28）：新增 `deploy/validation/compose.one-node.yaml`，在独立 Compose project/卷中同时启动 PG 18.6、Kafka 4.3.1、ES 8.19.22、Keycloak 26.7.4，端口只绑定 loopback（15432/19094/19200/18181）。全新栈上应用 12 个 PG migrations 并重复运行；`tuba_runtime` DML-only 账号连接 `tuba` 成功；Keycloak 专用 DB 初始化重复执行、realm discovery 在重启后仍成功。`tuba-topic-admin` 连续两次 reconcile 15 个 bounded topic，读回为 1 partition/1 replica、Raw 2 MiB/24h。ES `apply_elasticsearch_assets.sh` 对全部模板重复 PUT；空集群不再因 anomaly mapping 对通配空集执行 PUT 而 404，创建 anomaly index 后也验证 mapping 更新，模板为 1 primary/0 replica。验收完成后删除隔离 Compose 容器/卷/网络，product PG/Kafka/ES 保持 healthy，未访问 248。该证据是开发版本组合的 disposable 初始化，不等于目标主机安装或 A03 容量批准。
- O02 Keycloak 运维补充（2026-09-28）：`deploy/keycloak/compose.postgres.yaml` 要求显式注入 PostgreSQL bootstrap user/password、Keycloak DB password 和 realm admin credentials；缺值时 Compose 配置失败，不再给该 profile 设置弱默认密码。新增 `scripts/update_keycloak_web_client.py`：变更前必须提供非空 Keycloak PostgreSQL dump；脚本导出既有 realm 到当前账号独占 ACL/0600 快照目录，拒绝 Direct Access Grants 已开启的客户端，更新既有 `tuba-web` redirect URI/origin 并读回校验。隔离 Keycloak 26.7.4＋PostgreSQL 18.6 中完成首次更新及同值重复更新，两次均通过；PG dump 为 253,381 bytes，realm export 写入受限临时目录。只使用隔离 project/volume，未连接 product PostgreSQL 或 248。完整依赖栈的一次性初始化已在下方补充验收；O02 仍未关闭，目标主机安装和部署验收仍待完成。
- [ ] O03（O/G）配置反向代理、TLS、私有监听、Kafka/ES/PG 最小服务权限；验证凭据不会出现在日志或制品。当前开发验收按用户要求仅使用 HTTP，不配置证书；TLS 明确延至生产准备阶段，不作为本阶段开发阻塞项。包构建新增 `scripts/verify_tuba_package_stage.ps1`，只允许固定文件清单进入制品，并要求 manifest 敏感环境值是 `${ENV_NAME}` 引用、示例凭据是占位符。双平台制品各含 17 个 allowlist 文件；校验要求所有 manifest 环境引用都在样例环境文件中声明、每个 ES 服务使用不同的 key 变量；归档不含实际 `.env`、local-dev realm 或 realm 导出。日志核对用假的 `DATABASE_URL` canary 启动 API 并用 Kafka canary 调用安全管理 CLI，捕获输出均未含 canary 值。PostgreSQL 应用/迁移 DSN 分离、专用数据库 DML-only `tuba_runtime` 账号已在一次性 PostgreSQL 18.6 中验收；ES 各服务角色、跨 namespace/职责拒绝及轮换已实测；Kafka ACL 和 loopback 监听边界亦有隔离运行证据。Vite 开发代理现将 `/api/v1/ingest` 单独转发至 loopback ingest，其余 `/api` 转发到 loopback API，隔离验收确认路径、方法、body 和 Authorization 均保留；RC18 包内 `tuba-web` 的首页、SPA fallback、OIDC 配置、API/ingest 到 mock upstream 的 method/path/query/body/Authorization 转发及 readiness 均有实际进程验收（`scripts/verify_packaged_web_proxy.py`）；真实目标网络边界和服务安装后验收仍待完成。A03 未定前不部署到 248，因此 O03 保持未关闭；TLS 不是当前开发验收门槛。
- O03 ES 授权验收补充（2026-09-28）：新增 `scripts/manage_elasticsearch_api_keys.py`，为 API、Raw Indexer、Quarantine Indexer、Standard Indexer、Analysis Sink 按 namespace 创建独立角色与 90 天 key；keyring 文件限制为当前 OS 账号可读，bootstrap key 不落盘，支持重叠轮换与按旧 keyring 撤销。新增一次性 XPack 开启的 ES 8.19.22 验收 Compose profile（HTTP，仅 127.0.0.1:19200）和 `scripts/verify_elasticsearch_api_keys.py`。真实验收覆盖五个服务职责内允许操作、跨 namespace 拒绝、跨数据集/读写职责拒绝，以及新旧两代同时有效→撤销旧代→新代仍有效。全部断言通过；生成的 key 与测试索引已撤销/删除，隔离容器、卷、网络已清理；现有 product ES 未接触。ES 不提供 `manage_aliases` 内置 index privilege；管理工具使用已在 ES 8.19.22 实测的 `indices:admin/create`、`indices:admin/aliases*`、`indices:data/write/bulk*`、`indices:data/write/index:op_type/create` action patterns，并将资源范围限制到服务自身 dataset/namespace。跨 namespace、跨职责写入、Raw/Standard mapping 修改与物理索引删除的拒绝均实测通过。四个索引服务已通过实际进程独立 key 写入验收，API 实际查询授权见后续运行验收。O03 仍未关闭：生产 TLS/反向代理/监听边界尚未验收（开发阶段继续 HTTP）；A03 未定前不得部署到 248。
- O03/O04 真实服务进程补充（2026-09-28）：新增 `deploy/validation/compose.analysis-sink-security.yaml` 与 `scripts/verify_indexer_identities_runtime.py`，使用唯一 Compose 项目名、临时卷及仅 loopback 端口启动隔离 Kafka 4.3.1 和启用 XPack 的 ES 8.19.22。脚本自动生成短期 ES bootstrap 密码和五类 namespace 专属 key，构建并启动真实 Raw Indexer、Quarantine Indexer、Standard Indexer、Analysis Sink；向各自 Topic 投递合同有效的原始、隔离、标准和 anomaly 记录，并以管理员只读确认四个目标文档写入。四个消费组均确认 ES 成功写入后 offset lag=0，四进程收到平台中断信号后正常退出；Analysis Sink key 跨 namespace 写入收到 403。2026-09-28 完整流程通过，临时 key 主动撤销，隔离容器/卷/网络回收，未连接现有 product ES、Kafka 或 248。服务级身份写入证据覆盖四个索引服务；API key 尚未由真实 API 查询请求验收。O03 仍待 API 服务级验收及运行监听/TLS边界决策；O04 的 Launcher `stop` manifest 验收曾被自动审批拦截，不重试同一操作，仍需在可接受的验收路径中完成 Launcher 监督进程停止验证。A03 未定前不得部署到 248。
- O03 API 服务级身份补充（2026-09-28）：新增 `scripts/verify_api_es_identity.py`，复用唯一临时 Compose project 的 `api-identity` profile 启动隔离 PostgreSQL 18.6、Keycloak 26.7.4、Kafka 4.3.1 与安全 ES 8.19.22；脚本逐个只执行 migration Goose Up 区段，在临时 DB 建立 local-dev realm analyst subject 的 `tenant_a` membership，以管理员仅写入一条临时 network 事件并通过 `refresh=wait_for` 保证可搜索。真实 `tuba-api` 使用 API 专属 ES key，通过 Keycloak RS256/OIDC 验证 analyst token、查询 membership 并返回该租户事件；无 active membership 的 auditor token 返回 403，API key 对其他 namespace alias 搜索也返回 403。API 收到中断信号后正常退出；key 主动撤销，临时卷/容器及测试目录清理。全流程通过；没有连接现有 product 服务或 248。API 本地查询与隔离授权已覆盖，生产代理/监听范围仍待定，A03 门槛仍有效。
- O03 Loopback listener 收口（2026-09-28）：`HTTP_LISTEN`/`API_LISTEN` 默认改为 `127.0.0.1`，并拒绝 wildcard、非 loopback IP、非法端口；双栈 IPv6 loopback `::1` 可用。config tests 覆盖默认地址、显式 loopback、wildcard/外部 IP/端口 0 拒绝。双平台服务 manifest 原已显式 loopback；基础 Compose 与所有 disposable validation Compose 的 host published ports 核对均为 `127.0.0.1`，Kafka 容器间 listener 不发布到宿主机；当前运行的 product Kafka/PG/ES 通过 `docker ps` 读回确认均只发布到 `127.0.0.1`。RC11 双平台包已通过 allowlist 安全审计；全量 `go test ./...` 通过，Ubuntu 24.04 网络隔离包验收由 RC10 升级到 RC11 并成功执行 standalone rollback。部署手册记录开发 HTTP 下的代理/访问边界。此项不配置 TLS，也不等同目标主机防火墙/反向代理运行验收；O03 未关闭。
- O03 Kubernetes listener 显式例外（2026-09-28）：修复 loopback 默认值与 Helm Pod 网络监听冲突。新增 `TUBA_ALLOW_NON_LOOPBACK_LISTEN`，仅精确布尔值 `true` 才允许 wildcard 或非 loopback IP；空值仍拒绝外部绑定，非法值失败关闭，主机名形式仍拒绝。Helm 只在 API/ingest workload 显式设为 `true`，`.env.example` 和 Launcher 单机 manifest 继续 loopback。`go test ./internal/config`、全量 `go test ./...`、`helm template tuba deploy/helm/tuba` 均通过；渲染结果确认该 opt-in 仅出现在 API/ingest。该项解决代码与模板的地址契约冲突，不等于 TLS/代理/集群网络验收，O03 仍未关闭。
- O03 Metrics listener 边界与 Helm 对齐（2026-09-28）：复核发现 Python analysis worker 示例默认 `:9090` 对全网卡监听，Go worker 的 metrics override 未校验；Helm `analysisSink` 还把 Go 二进制的 `ANALYSIS_SINK_METRICS_LISTEN` 错写为未读取的 `METRICS_LISTEN`，探针/容器端口 9090 与二进制实际默认 19094 不一致。Go `telemetry.ListenAddress` 现复用 loopback/精确 opt-in 校验，7 个 Go worker 的默认 metrics 端口均只监听 loopback；Python worker 默认改为 `127.0.0.1:9090` 并实行相同 wildcard/外部 IP opt-in 与非法值拒绝。Helm 为 Python analysis worker 显式开 Pod 监听；analysis sink 改为 `ANALYSIS_SINK_METRICS_LISTEN=:19094` 并将 metrics/probe/container port 对齐到 19094。chart NetworkPolicy 的 scrape 入站仍限配置的 monitoring namespaces。全量 `go test ./...`、在 `python/` 执行 `uv run --project . --locked python -m unittest discover -s tests`（9 项）、`helm lint`、渲染读回通过；O03 仍未关闭。
- O01/O03/O04 RC14 双平台包回归（2026-09-28）：包含 Go worker metrics listener 校验的 Windows/Linux amd64 RC14 均通过包安全审计；哈希 Windows `622c4538117ac428bc787fc2970cef1b2b535e1451dbd3790b3da3296f8d8609`、Linux `1ed2dde6b237d5373d9d30ee29afd4c9cdb81865f398fb3fea39b39b28d2fb55`。Windows 包内 9 服务 manifest 与 listener opt-in 正反例通过；Linux 一次性 Ubuntu 24.04 容器 RC13→RC14 升级、manifest validate、服务账号/权限、Launcher lifecycle/rollback 通过，同时 sidecar 错指和 symlink archive 拒绝用例仍通过。未连接 248。
- O04 metrics manifest preflight 与 RC15（2026-09-28）：Launcher 检查扩展至 Python `METRICS_LISTEN` 及各 Go `*_METRICS_LISTEN`；新用例确认 wildcard 没有显式 opt-in 时 manifest 被拒，设置精确 `true` 时通过。双平台 RC15 包 SHA-256：Windows `cdf76207376305f04260f37884dd8e6abde4c4c70ed71320c132d53b5afc2186`、Linux `863a3575608b46e2e4dfeb3aa8b20fb0a81c8cfa00874128c38b8ff3df1956d1`。Windows 包内 9 服务 manifest 与 HTTP/metrics wildcard 正反例验证通过；Ubuntu 24.04 隔离容器 RC14→RC15 安装升级、权限、Launcher lifecycle/rollback、sidecar 错指与 symlink 拒绝均通过。该证据仍不覆盖 Windows 专用账号真实安装或 Launcher 管理真实产品服务。
- [ ] O04（G）实现 live/ready、依赖重连、优雅停止、有界队列、配置校验；修复盘点确认的启动配置问题。进行中（2026-09-28）：`internal/config.Load` 对速率、批大小、批字节数、重试次数、等待/退避时长执行范围校验，非法值不再静默回退；API ready 检查 PostgreSQL/Elasticsearch，ingest ready 检查 PostgreSQL/Kafka broker；raw/normalizer/standard/quarantine indexer、control worker、analysis sink、source adapter 已接入独立 loopback 运行探针，ready 随 worker 生命周期变化；standard indexer 增加 16 MiB 默认/64 MiB 上限的批字节预算，Kafka 消息 offset 不提前提交；Go 入口统一接入 Unix SIGTERM 与 Windows Interrupt 生命周期上下文。新增 API/ingest 30 秒请求 deadline、HTTP 读写/空闲超时，以及所有运行时 PostgreSQL pool 的 15 秒 statement timeout（1 秒至 5 分钟）和 5 秒 ping 上限。逐消息同步的 raw/normalizer/quarantine/analysis worker 不维护应用内积压队列，standard indexer 单批同时受条数和字节预算约束。PG/Kafka 依赖断连恢复已有隔离运行证据。修复 API/ingest 的停机竞态：新增 `internal/lifecycle.ServeHTTP`，等待活动 handler 完成后才返回，超时强制关闭连接；API 与 ingest 统一使用该入口。生命周期测试覆盖活动请求 graceful drain、超时强制关闭与监听失败；`go test ./...` 通过。新增真实 Raw、Quarantine、Standard Indexer、Analysis Sink、API 隔离运行验收：服务写入/查询后 offset 与租户检查符合预期，平台中断信号触发正常退出。2026-09-28 后续已验证 Launcher `run` 通过 manifest 监督真实 control-worker：PG/Kafka 断连恢复时 readiness 为 200→503→200，强制结束一次隔离子进程后 Launcher 自动重启 worker；向 Launcher 发送平台中断信号后产品子进程优雅退出，独立 status 确认为 stopped。仍缺目标主机重启恢复验收；此前被拦截的 manifest `stop` 命令未重试。O04 尚不勾选。
- O04 验收补充（2026-09-28）：`internal/ingest/server_test.go` 覆盖依赖不可用→恢复→再次不可用时 `/health/ready` 的 503→200→503；`internal/api/server_test.go` 覆盖 DB/ES 探测对象缺失时拒绝 ready。独立 Compose project（PG18.6/Kafka4.3.1，15434/19094 端口）运行真实 `tuba-ingest`，再经 loopback fault proxy（15435/19095）分别关闭既有连接并拒绝 PG/Kafka 新连接：两项依赖均测得 ready `200→503→200`，无需重启 ingest；初始依赖未启动时 live=200/ready=503，依赖先后启动后 ready 恢复 200。Ctrl-C 后进程退出、18089 端口释放。验收容器/卷/网络/监听端口已清理，现有 `product` Compose 的 PG/Kafka/ES 均保持 healthy。自动审批审查拒绝了用 Launcher manifest 执行该临时 ingest 的 `stop` 演练，工具只返回 `blocked by policy`，未提供更具体原因；O04 的 Launcher 实际停止验收仍待完成。该运行证据不替代目标环境演练。
- O04 剩余 worker 停止验收补充（2026-09-28）：新增 `scripts/verify_remaining_worker_shutdown.py`，在独立 Compose project 的 Kafka/PostgreSQL 上应用 migration Goose Up 后，构建并启动 normalizer、control-worker、source-adapter；三者各自 `/health/ready` 返回 200 后发送平台正常中断信号，全部在 15 秒界限内以 0 退出。临时容器、卷、Topic 和测试目录清理；未连接现有 product 服务或 248。至此 ingest、API 和主要数据 worker 已有直接进程生命周期证据。Launcher 监督器的完整运行验收仍待通过可接受路径完成，不将 helper 子进程测试当作产品部署验收。
- O04 Launcher stop 控制面测试补充（2026-09-28）：新增 `TestStopRequestsManagedRunnerAndWaitsForExit`。Go 测试启动仅等待临时 `stop.request` 的 helper 子进程，调用 `internal/launcher.Stop` 并验证请求文件发送/清理、Stop 等待 helper 退出及 `runner_pid=0` 状态落盘；`go test ./internal/launcher -count=1` 通过。该测试覆盖 Launcher Stop 控制函数和竞态，不启动 TUBA ingest/业务服务，也不等价于被策略拦截的远程/manifest 运行验收；O04 仍需通过可接受的完整运行方式完成 Launcher 管理进程的验收。
- O04 Windows supervisor 回归测试补充（2026-09-28）：新增 `TestWindowsSupervisorGracefullyStopsManagedChild`，在 Windows 上通过 Launcher `Run` 启动隔离 helper 子进程；子进程安装 Ctrl-Break 清理处理器并写入停止标记，测试写入内部停止请求后确认 supervisor 在 5 秒内退出、子进程完成清理、最终状态落盘为 stopped 且 `runner_pid=0`。本机 `go test ./internal/launcher -count=1` 通过。该证据覆盖 Windows supervisor 的真实进程组/信号路径，但 helper 不是 TUBA 服务，仍不替代产品 manifest 下的完整服务运行验收；O04 保持未勾选。
- O04 control-worker/Launcher 隔离运行验收补充（2026-09-28）：扩展 `scripts/verify_remaining_worker_shutdown.py`，使用独立 Compose project/临时目录及真实版本 `tuba-control-worker`，由产品 `tuba-launcher run --manifest` 启动监督；分别停止并恢复 PostgreSQL 与 Kafka，worker 均在不重启下完成 readiness 200→503→200。随后只对临时状态文件中指向本次构建 worker 的 PID 执行强制退出，确认 Launcher 自动重启真实产品 worker、readiness 恢复；再向 Launcher 进程发送平台 SIGTERM/Ctrl-Break，Launcher 等待产品 worker 优雅退出，独立 status 命令确认 `TUBA launcher is stopped`。normalizer/source-adapter 另行启动并验证平台信号退出。全量 `go test ./...`、脚本语法和完整隔离运行脚本通过，临时容器/卷/网络/日志/manifest 清理。该验收不调用此前被拦截的 Launcher `stop --manifest` 路径，也未连接 product 服务或 248。O04 的本机真实服务监督、自动重启及停止证据已补；仍缺目标主机重启恢复，阶段 2 不关闭。
- O03 开发 HTTP 反向代理验收（2026-09-28）：更新 `web/vite.config.ts`，将 `/api/v1/ingest` 路由到 loopback ingest `127.0.0.1:8080`，其余 `/api` 路由到 API `127.0.0.1:8788`。新增 `scripts/verify_dev_http_proxy.py`，在两个 loopback mock upstream 和随机本机 Vite 端口上验证 `/api/v1/me`、`POST /api/v1/ingest/events` 的 upstream、method、path、body 与 Authorization header；全流程通过并清理临时进程。`node node_modules/vite/bin/vite.js build` 在 `web/` 目录构建通过（有既存大 chunk 提示）。这是开发服务器代理验收，不代表安装版静态 Web/目标主机代理验收；HTTP scope 不引入证书。
- O01/O03/O04 RC16 双平台包验收（2026-09-28）：`scripts/package_tuba.ps1` 生成 Windows/Linux amd64 RC16，并通过 17 项 allowlist、secret reference、示例凭据占位符及 ES key 唯一性校验。SHA-256：Windows `e31f424b17e31383f74e948d903a9569416049b1d0a1109b501443ef4afa0729`；Linux `7f94e94e79b83596b5c8cae4b1df40b29b1a7533355a61d2a5f38ed2de10531e`。Windows 包在本机由包内 Launcher 对 9 服务 manifest 执行 validate，并验证 HTTP/metrics wildcard 默认拒绝、显式 opt-in 接受。Linux 包在无宿主数据挂载的一次性 Ubuntu 24.04 容器内完成 RC15→RC16 升级、非 root 专用账号/权限、校验和、Launcher lifecycle 和 rollback；危险 sidecar 与 symlink archive 拒绝用例亦通过。没有连接 248；裸机/专用 Windows 账号安装仍未验收。
- O01/O03 RC18 manifest 迁移与打包 Web 运行验收（2026-09-28）：修复升级仅首次安装复制新 example、旧安装清单缺少新增服务的问题。新增 `tuba-launcher merge-manifest`：以服务名比较当前 manifest、当前版本 example 与新版 example，只补入新版首次引入的服务；既有服务环境/监听/退避覆盖、顶层目录配置和管理员显式移除的既有服务均保留。候选清单会先把当前 release 路径映射到新 release 并完整校验，校验成功才输出候选。Linux/Windows 安装器在停止旧进程前生成并校验候选；切换 release 后备份旧 manifest 并替换，提交失败时恢复 previous release。拒绝覆盖已有迁移候选或备份。`go test ./...`、Launcher `go vet`、Windows 安装器 PowerShell 语法检查、Linux 安装器 `bash -n` 均通过；合并单测验证新服务加入、既有覆盖保留、管理员移除项不复活、非法新服务时旧清单原字节不变且不产出候选。RC18 双平台包通过 allowlist 与 SHA-256：Windows `a65142803d3463b348429e61b24ae510d09012e9562d08b4cc03560950231e47`，Linux `8d4811af25b9c9e998352b43e7730e6b3ff7fe373850bb1023d83a07aa47ad37`。Windows 包内 Launcher 校验 10 服务清单通过；包内 `tuba-web.exe` 实际启动后，首页、`/overview` SPA fallback、`/config.js` 运行时 OIDC 配置、内部 ingest 路由拒绝及上游不可用时 readiness 503 均通过。 随后在同一 RC18 Windows 包进程上使用本地 Python 标准库 mock upstream 复验，API `GET /api/v1/me?expand=roles` 与 ingest `POST /api/v1/ingest/events?source=smoke` 的 method、完整 path/query、Authorization 和 ingest body 均原样到达正确 upstream；两 upstream ready 时 gateway readiness 为 200，内部 ingest 路由仍为 404。新增 `scripts/verify_tuba_linux_web_package.sh`；Ubuntu 24.04 隔离容器运行 Linux RC18 包内 `tuba-web`，验证 hash、live、静态首页、SPA fallback、OIDC runtime config、内部路由拒绝及上游不可用时 readiness 503。Linux 安装器首次安装验收：SHA-256、`tuba` 专用非 root/nologin 身份、state/env/manifest 权限、current 链接、10 服务及非 root Launcher validate 通过。另以 9 服务 RC16 清单生成 RC18 候选，新增 `web` 后 10 服务校验通过。没有连接产品依赖或 248。此前自动审查拦截过 Launcher `stop --manifest` 验收，本轮未运行会触发该路径的 Linux 升级安装器；Ubuntu 裸机和 Windows 专用账号目标安装、真实安装器升级/回滚仍待验收，O01/O03 不勾选。
- O01/O04 RC20 主机重启状态加固（2026-09-28）：Launcher state 新增 `runner_identity`，Linux 由 kernel boot ID 与 `/proc/<pid>/stat` 启动时刻组成，Windows 使用进程创建时间；`start`、`stop`、`status` 只有在 PID 与身份同时匹配时才把持久状态视为当前 supervisor，避免主机重启后 PID 被其他进程复用导致 Launcher 拒绝恢复。Supervisor 创建 state 时若无法取得自身身份会失败关闭，`start` 的成功等待也要求非空且可读回匹配的身份。新增单元测试验证当前身份匹配、伪造旧身份拒绝和 state 持久化；新增 `scripts/verify_launcher_boot_recovery.sh` 用于 Linux 真实进程演练旧 supervisor 消失、PID 指向无关存活进程且身份属于旧启动后可重新监督服务。串行 `go test -p 1 ./...`、Launcher `go vet`、Windows release junction 首装/升级/回滚回归均通过。RC19 是加入初版身份字段后的中间包，哈希为 Windows `bf351666d9316a80b7e185c9761a57bd25973e9be50a78328ef287cb4e2d38dd`、Linux `8ddd7cd9c53798762cbe5b9a10a77a1bb70972ebdca13a43aeaa0fa876335d02`；初轮加固后生成 RC20，哈希为 Windows `452a79e8524166a791985e288b953e72ca158bb5aa1d7d54e1a93361a4c73e67`、Linux `d8b78068fd90a5bfed4fbb65f37c807d6fe5c949c1c0b100e4e7c97d13cb67eb`，两包均通过 24 项 allowlist/secret reference 审计。RC20 Windows 包已通过实际包内 Launcher 的 10 服务 manifest 校验、静态 Web、API/ingest HTTP 代理请求保真和 readiness 验收。Ubuntu RC18→RC20 升级首次实测发现 `merge-manifest` 以非 root 账号在只读 `/etc/tuba` 创建验证临时文件而失败；安装器已改为在 `tuba` 私有 state 目录生成候选、由 root 提交到配置目录。修复后在一次性 Ubuntu 24.04 容器完整通过错误 sidecar/符号链接包拒绝、首装、升级、专用账号与权限、10 服务 manifest、Launcher 生命周期和独立回滚。RC20 包内 Linux Launcher 还通过真实 boot-recovery smoke：旧 supervisor/child 强杀后，将 state PID 指向存活无关进程并保留旧身份，`start` 能拒绝伪存活状态、恢复监督，SIGTERM 后落盘 stopped。以上容器无网络、无产品数据挂载且未连接 248。仍缺目标裸机跨真实主机重启验收和 Windows 专用账号安装；O01/O04 保持未勾选；A03 未定，未部署到 248。
- O01/O04 RC21 旧状态升级收口（2026-09-28）：复核发现 RC18 及更早版本的 state 没有 `runner_identity`，重启后若 PID 被复用，RC20 的旧格式兼容分支仍可能误判。现将非零 PID 但缺失身份的 legacy state 视为不可验证的 stale state；受支持的安装器升级会在激活新二进制前停止旧 supervisor，因此不依赖该不安全兼容行为。正常停止同时清空 PID 与 identity，测试增加 legacy PID-only 拒绝断言。RC21 SHA-256：Windows `fd47c521226e77f4d259953ada7fe3c498c46a166460dd3cb57c472f168b123c`，Linux `cb5f2f39ca7912016d6b4cea6707b2747ce46192b609f0e283f06ce4ebef4db2`；双平台 24 项包审计通过。Windows RC21 包内 10 服务 manifest、Web、API/ingest HTTP 代理和 readiness 验收通过；一次性 Ubuntu 24.04 中 RC20→RC21 安装升级、专用账号/权限、Launcher 生命周期及独立回滚通过。boot-recovery smoke 使用 RC21 包内 Launcher，强杀临时 supervisor/child 后删除 state 的 identity 并把 PID 指向存活无关 shell，确认 Launcher 拒绝 legacy 伪存活状态、重新监督服务且 SIGTERM 后落盘 stopped。部署手册补充：产品不注册 systemd/Windows Service，目标主机重启后由受控运维入口或后续 Management Agent 调用 Launcher；容器演练不冒充裸机重启。当前候选为 RC21，目标裸机和 Windows 专用账号门槛仍未满足，O01/O04 不勾选。
- O01/O04 目标机验收续进（2026-09-28）：修复安装器 release ACL 后生成的 RC22 已在 169 实机从 RC21 升级成功，`current`/`previous` 分别指向 RC22/RC21，manifest 含 10 个服务，release 示例文件管理员可读，`WIN-169\tuba` 保持非管理员，Windows Service 数量为 0。RC22 SHA-256：Windows `9c5869b873e7546fe9e0bc9706775f5b1cf06964b797569a49853cf561b5abbf`、Linux `0c27d90d90a714a322a2dc329690249e5e041467bb42a836305111d2f3a31b26`。首次 RC21 首装暴露的 ACL 缺陷已修复：递归授予继承 ACE 会使部分 Windows 版本子项有效 DACL 为空；安装器现设置 release 根 ACL 后重置子项以继承根 ACL。2026-09-28 通过 WinRM HTTPS（仅跳过本机到目标的证书链/名称校验）再次只读核验：`tuba` enabled、非管理员，`current` 指向 `stage2-rc22`，安装后的 10 服务 manifest 由 RC22 Launcher 实际执行 `validate` 返回成功；manifest/env ACL 为 Administrators/SYSTEM 完全控制、tuba 仅分别读/完全控制。主机端口 5985/5986 可达；HTTPS 路径可用，无需修改本机 TrustedHosts。仍缺由 tuba token 实际启动/停止 Launcher 管理的 helper 与真实进程验收。先前 scheduled task 返回缺少 `SeBatchLogonRight`；给该 SID 临时授权的 LSA/secedit 方式被自动审批策略拦截，未改动该权限或保留任务。下一步应在获准的 Windows 管理终端短时添加并撤销 Batch Logon right，或通过现成的受管非管理员交互登录会话验收。O01/O04 仍不勾选。
- O02 初始化编排回归（2026-09-28）：此前在当前 Windows 开发机通过 Git for Windows Bash 执行时退出码 49，跟踪显示首次调用 initializer 提前失败，尚未归因为脚本还是 Git Bash 兼容问题。随后在 71 的 Linux 环境重跑 `scripts/test_initialize_tuba_single_node.sh`，退出码 0，报告 `Single-node initializer orchestration test passed`：固定顺序、realm discovery 校验、非法 namespace 拒绝和 A03 未批准时拒绝 production topic profile 均通过。测试仅传入 mock 数据库/Kafka/ES/Keycloak 地址，initializer 中的步骤和 curl/python 均为临时 mock；未连接或改动 71 上 Kubernetes、Kibana、MongoDB、Filebeat 等业务，也未访问 248；唯一上传文件位于随机 `/tmp/tuba-priority-verify-*` staging 目录，测试及清理后已回收。该回归不等于 O02 的目标机安装验收。
- [x] O05（O）接入 Prometheus/Grafana、服务日志轮转、磁盘及消息保留保护；出具首期 dashboard 和告警规则。248 部署验收见 2026-09-28 记录；2026-09-29 复核 Prometheus/Grafana 管理器运行正常、抓取目标 6/6 UP，capacity guard 保持 ready。按用户要求，邮箱接收地址虽已指定，但 SMTP/Alertmanager 外发通知暂缓，不影响本地告警规则/dashboard 的完成；外发通知单独保留后续项。
  - O05 日志限额进展（2026-09-28）：Launcher 管理的每个服务日志现在按 16 MiB 轮转，保留 3 个旧文件；Launcher supervisor 日志在下次启动前检查并轮转。RC23 Windows/Linux 包已构建并通过 24 项 allowlist/secret-reference 审计；SHA-256：Windows `ea6c21294cecd9d904ecc71b5e66bd775b14f4802857513fc74923869f697655`，Linux `a2e9b8e92b6a7a7ccdb3cab759f910637cda080625de190c4ba56fade296b3ed`。Windows 包内 10 服务 manifest、Web/HTTP proxy 验收通过，Linux 包在 71 临时解包后完成 SHA-256 和 10 服务 manifest validate。`go test ./...` 与 `git diff --check` 通过。仍缺单节点 Prometheus/Grafana scrape/dashboard/alerts、node exporter 磁盘水位、Kafka lag 监测和 A03 决定后的 retention guard；RC23 尚未安装到目标机。
  - COL-07/O04 worker 故障演练（2026-09-28）：在 71 以 RC23 Linux 包内真实 control-worker、normalizer、source-adapter 与 Launcher，通过一次性临时 Docker shim 建立唯一命名网络及 Kafka 4.3.1/PostgreSQL 16 disposable 容器；没有使用或停止 71 上原有 Zeek、MySQL、MongoDB 容器。脚本先后验证 Launcher 管理的 control-worker 在 PostgreSQL 与 Kafka 分别停止时 readiness 200→503、恢复后 503→200；强杀其隔离子进程后 Launcher 自动拉起新 PID 且 readiness 恢复；向 Launcher 发 SIGTERM 后 worker 优雅退出、状态为 stopped；normalizer/source-adapter readiness 和平台信号退出均通过。首轮核心断言通过后暴露 Python 3.6 `Path.unlink(missing_ok=...)` 清理不兼容，修正后复跑最终退出码 0，输出 `PASS`；临时 Compose 网络/容器已删除，复核 71 仅原有四个业务容器仍运行。该演练不覆盖 Kafka lag/Zeek spool 积压、重复/跨 offset 投递、轮转、磁盘满、日志覆盖、Topic 重建或目标裸机重启；COL-07 仍未关闭。
  - O05 环境只读核查（2026-09-28）：248 未发现 Prometheus、Grafana、node exporter 或 Kafka exporter 进程，3000/9090/9093/9100 也无监听。日志轮转代码与 RC23 包已具备，单节点 scrape、dashboard、磁盘/lag 告警和 retention guard 尚未部署；部署前需先满足 A03 存储门槛。
  - O05 指标合同修订（2026-09-28）：核对 `internal/` 与 `python/tuba_analysis` 的真实埋点后，修正 Helm PrometheusRule/dashboard 中失效的 ingest accepted/rejected 与 analysis watermark/DLQ 名称，告警改为实际暴露的 Kafka 写失败、source adapter 重试/DLQ 落盘失败及 Raw/domain 索引写失败指标。OBSERVABILITY.md 补充当前真实指标目录，并明确 Kafka lag 与磁盘空间依赖外部 exporter；身份/格式拒绝和限流当前无 Prometheus 计数，未伪造相应拒绝率告警。本轮只改配置和文档，尚未运行 Helm render，也未部署 exporter/Prometheus/Grafana。
  - A03 最终边界（2026-09-28）：当前 50 GiB 单节点仅允许 Zeek 四类日志；Kafka 24h，ES 7 日，Filebeat queue 每路 256 MB，archive stage 6h，根盘 70/75/80% 水位。合同和生成资产已更新；实际 retention guard 与告警部署转入 O05。
  - O05 容量保护部署（2026-09-28）：248 已部署由产品 CLI 自行启停的 `tuba_capacity_guard.py`，未注册 systemd。守护进程每 60 秒检查根盘和 ES，只处理 namespace `zeek_validation_20260927_001` 的 Raw/Quarantine/八领域日索引，保留当天及前 6 个 UTC 日；在 `127.0.0.1:19100` 提供 live/ready/metrics。ES persistent settings 已读回 low/high/flood-stage=`70%/75%/80%`，80% 时由 ES `read_only_allow_delete` 机制保护写入。部署后 ready=200，磁盘 50.8%、level=normal、候选=0，管理 CLI status 显示 running。首次版本的删除范围过宽，在收窄 namespace 前按 7 日规则删除了 `tuba-v1-uim-network-tenant_a-g1-2024.07.14`（4 条历史样例、无备份，已审计且不可恢复）；随后立即改为仅当前批准 namespace 并重启读回。O05 继续开放，Prometheus/node/Kafka exporter 正在部署，Grafana 与外部通知链路尚未完成。
  - O05 监控部署（2026-09-28）：248 实际运行 Prometheus 3.5.0、node_exporter 1.9.1、Kafka exporter 1.9.0、Grafana 12.2.0，及产品 CLI 管理的 16 MiB×3 轮转进程；均以专用 nologin 用户启动，未注册 systemd，监听只绑定 loopback（Prometheus 19090、node exporter 19101、Kafka exporter 19102、Grafana 13000；capacity guard 19100）。Prometheus 数据保留上限 15 日/1 GiB；Prometheus 配置和 8 条告警规则经 promtool 检查，5/5 当前抓取 target 为 UP。Grafana health 200、TUBA Prometheus datasource 和 `TUBA Single Node Operations` dashboard 均已通过 API 核验。Prometheus/node exporter 官方 SHA-256 匹配；Kafka exporter GitHub v1.9.0 release 未发布 digest，已与工作站下载文件 SHA-256 对照；Grafana 包匹配官方 `.sha256` sidecar。Kafka observer 身份 ACL 为只读且按当前 Zeek group profile 收窄。监控发现当前 `tuba-source-adapter` 与 `tuba-normalizer` 状态 stopped/stale；conn group lag 最新现场快照约 11.2k 且无活动 member，多个来源组有积压，绝未重置 offset。Prometheus 已可本机呈现 lag/inactive-with-backlog 告警；尚未重启这些数据面服务。外部通知尚缺 Alertmanager receiver 配置和接收端信息，因此 O05 仍开放。
  - O01/O04 169 非管理员实进程验收复查（2026-09-28）：远程管理主机的 WinRM 5985/5986 当前 TCP 不可达，SSH 22 可连接但提供的 Administrator 凭据未通过 SSH 认证；没有改账号、登录权或服务状态。先前计划任务验收已证明 `tuba` 缺少 `SeBatchLogonRight`，临时授权曾被策略拦截。需要恢复获准的 WinRM 管理通道，或让运维提供一个现成的 `WIN-169\\tuba` 交互会话，再完成 helper 的真实启动/停止验收。

## 阶段 3：可信原始接入与证据（P0；依赖 C01、C06、O01–O04）

- [ ] I01（G）实现来源注册、凭证绑定、namespace/dataset/配额授权和可信上下文；拒绝客户端越权声明。首版 API 与 ingest resolver 已实现；source reset 与真实 Collector 联动、发布版本管理和验收仍待完成。
- [ ] I02（G）实现 `/api/v1/ingest/events`，Kafka 确认后返回 receipt；明确 400/401/403/413/429/503 行为。可信来源由 `internal/ingest/source_registry.go` 解析；仅允许数据库登记的来源凭证，Raw 路由仍需补齐错误合同、就绪状态和可靠性验收。
  - **2026-09-30 复核：错误合同只差一项，且那一项与设计基线冲突。** 实现实际返回的状态码：400 / 401 / **415** / 409 / 413 / 429 / 500 / 503；本项要求的 400/401/413/429/503 均已实现，**403 从未返回**。原因不是遗漏：`DESIGN-BASELINE.md` 明写「**授权失败、映射缺失、PG/Kafka/ingest 不可用和限流均为可重试错误**」，所以「已停用/未绑定的来源」被有意映射为 **503** 并保留 offset 重试（代码注释：a stale/revoked source remains at its input offset until explicitly resolved），而不是 403 永久拒绝。**需要定的问题**：本项列出的 403 是过时条目，还是应当把「未认证(401)」与「已认证但未授权(403，永久)」区分开？后者会改变适配器的行为——现在被撤销的来源会永远重试而不排空。另两处小偏差：**415 已实现但本项未列**；**429 没有测试断言**。
  - **2026-09-30 按用户决定实施（取「分开两种认识状态」的方向）**：`403` 现在是真的了。来源生命周期由布尔 `enabled` 改为三态 `active`/`paused`/`revoked`（迁移 `00014_source_lifecycle_state.sql`；`enabled` 保留并由 CHECK 约束与 `state` 保持一致，使旧二进制仍可运行且二者不会漂移）；resolver 不再在 SQL 里过滤状态，而是**读出来分类**（`ErrSourceRevoked` / `ErrSourcePaused` / `ErrSourceUnauthorized`）；ingest 两条路由把撤销映射为 **403**、暂停与未知映射为 503（未知在可信路由上仍是 401）；adapter 的 `permanentIngestRejection` **把 403 移出可重试类**，改为隔离并提交。
    理由与实证：撤销是确定性拒绝，重试不会改变答案，扣住 offset 只会把记录交给 topic 保留期且不留被拒证据——2026-09-30 采集端那个 ACL 被撤销的实例重试三天、49,144 条授权错误即同一形态。`paused` 与 `revoked` 的区分正是为此：前者保留 offset，后者隔离并前进。
    三条新测试均做**变异验证**：把 403 放回可重试类 → adapter 报 `ingest calls=9, want 1`；把两处 403 改成 503 → ingest 报 `want 403`。`go test ./...` 26 包 ok、0 FAIL，`go vet ./...` 干净。
    **已于 2026-09-30 上线到 248**（用户说明 248 是开发服务器、可直连、可直接停），现场验收见下节。
  - 就绪与可靠性两项其实已有证据：`TestReadinessTracksDependencyRecovery` 覆盖依赖断开/恢复的 200→503→200；可靠性验收由 COL-07/V02 非破坏性子集四项覆盖。
- [ ] I03（G/O）按 [COLLECTOR-DESIGN.md](COLLECTOR-DESIGN.md) 交付 Filebeat/Winlogbeat＋TUBA 管理和可信适配链路；按 COL-01–COL-15 的对应来源和迁移任务验收，不以设计文档代替实现。
- [ ] I04（G/D）实现 Raw 索引分支、原文 hash、按固定采集日期写入、访问控制和归档就绪状态。
- [x] I05（G）删除旧 `/api/v1/events/authentication` 入口及其专用 Kafka/indexer 路径，不提供兼容实现。
- [ ] I06（G/O）接入容量和消息过期保护；永久拒绝、过滤、归档滞后均有可查询状态。

阶段出口：确认接收即有持久消息；Collector 断线/重试不丢采集位置；原始证据独立可追溯。

Collector 细化任务（2026-09-27 已按成熟采集器方案重新定义；旧任务见 [历史清单](history/COLLECTOR-TODO-20260926.md)，历史编号不代表下列任务完成）：

- [x] COL-01（G/D，P0）定版接入合同：Beat 消息 schema、原始证据、来源上下文/Topic 绑定、transport delivery ID、source position、大小限制、三段确认、永久拒绝及重试。初版 schema/样例/Topic 合同已落在 `contracts/events/beat-ingress/1/`；OpenAPI 已引用 Beat 输入 schema，并说明 1 MiB JSON 对象与 adapter 的 Kafka source-position 编码。202 回执返回 source_context_id/source_position/payload_hash，adapter 校验三者后才推进来源 offset。ingest 已增加内部 adapter route，并按规范 Topic ID 从 PG 解析启用 source instance/context；adapter 配置只保留 Topic allowlist。确认 delivery position=`topic/partition/offset`，明确跨 offset 重发不去重。端到端故障验收及完整 OpenAPI 解析/兼容门禁仍未完成。**2026-09-30 复核与对齐：** 合同 `contracts/events/beat-ingress/1/contract.md` 有三处陈述与已验证的实现不符，已就地订正——(1) 状态行原写“Topic ACL 与 adapter 尚未部署”，实际早已部署并闭环；(2) 原文“除非来源适配器另行提供并验证稳定位置，不承诺跨 offset 去重”的条件**已被满足**：稳定位置增强已实现且对 Zeek/Windows 缺失即 400，实测同一记录换 offset+777 重发返回同一条 `receipt_id`、raw topic 中该 `raw_event_id` 只出现 1 次；(3) 原文“profile 准入隔离仍待实现”，实际 ingest 已对两类 profile 做 400 拒绝——**并显式记录了它与原文的差异**：原文写“写接入 quarantine”，实现是“400 → adapter DLQ”，两者都“不发布不可信 Raw”但机制不同，合同已改以实现为准。
  现有关键不变量均有测试覆盖并通过（`go test ./internal/sourceadapter ./internal/ingest ./internal/rawevent`）：ingest 5xx 时 **offset 保持未提交**（`TestProcessUntilCommittedRetriesIngestServerError`）、永久 4xx 才隔离并提交以免卡住分区（`TestProcessUntilCommittedQuarantinesIngestRejectedEvent`）、稳定位置强制且**跨 offset 去重**（`TestWindowsSecurityIngressRequiresStablePositionAndDeduplicatesAcrossOffsets`）、canonical hash 忽略采集器传输元数据（`TestCanonicalPayloadHashIgnoresCollectorLocationMetadata` 等）。
  **OpenAPI 兼容门禁已实现**（按用户决定另开独立脚本，不破坏原校验器的零依赖前提）：新增 `scripts/validate_openapi.py`，用 PyYAML 解析整个文档、**递归解析全部 `$ref`**（内部 JSON 指针解析到已解析的文档，外部 `.json` 引用要求文件存在且**复跑 `validate_contracts.validate_schema` 的同一套规则**）、检查每个 path 的方法与 responses、并要求 `BeatIngressEvent` 仍指向规范 Beat schema。已接入 `make contracts` 与 `make check`，前置依赖写进 DEVELOPMENT.md（与 helm/go/uv/pnpm 同类）。
  门禁自身的证据：正常路径通过（**42 paths、111 条内部引用、2 条外部引用全部解析**）；变异测试逐项确认能拦住漂移——内部引用指向不存在的组件、外部引用指向不存在的文件、Beat 引用被换成别的 schema、某 path 抽掉 responses、某 path 抽掉全部 HTTP 方法、openapi 版本被改，**全部被拦下**。其中一次“把 get 改名”的变异未被拦下，经核查是**变异无效**（该 path 还声明了 post，仍属合法），不是门禁漏洞，已单独用“抽掉全部方法”的变异复验并被拦下。
  **本项可勾选**。范围说明：`端到端故障验收`在此按 D1 定义的非破坏性子集计（见 COL-07/V02——**注意该子集现在并非"全部完成"**：五项里已做四项，剩"采集端断网后补齐"未做，属可做范围；破坏性子集归 D4 且依赖加盘扩容，不作为本项前置）。
- [ ] COL-02（G/O，P0）锁定 Filebeat/Winlogbeat 版本、OS/架构、Kafka 输出及磁盘队列能力；核对制品与再分发条件，建立带哈希的组件 manifest、下载/离线导入和统一目录包。已固定原型版本 8.19.0、Linux amd64 Filebeat 与 Windows amd64 Filebeat/Winlogbeat，manifest 校验值取自 Elastic 官方 SHA-512 sidecar；`scripts/package_managed_collectors.ps1` 实现下载/缓存/离线校验/打包且未执行。21 上现有 Filebeat 为 8.19.0，官方包授权审查、Beat→Kafka 与 broker/queue 兼容性及真实受管运行仍待验证。
  - **2026-09-30 复核：仍不能勾选，两个缺口且相互关联。**
    - **官方包授权审查没有做。** 现有记录只有一条设计原则（`COLLECTOR-DESIGN.md`：“默认支持从批准地址下载并校验，**不预设可任意重新分发厂商二进制**”），**没有对 Elastic 实际条款的核对结论**。这不是形式问题：`manifest.v1.json` 声明的三个制品都是 Elastic 8.19.0 包，缓存与再分发它们是否被允许直接决定下一步能不能做。
    - **统一目录包没有产出，打包路径从未端到端跑通。** `scripts/package_managed_collectors.ps1` 实现齐全（读 manifest、`-Offline` 校验、暂存 manifest），但 `dist/component-cache` 里**只有 1 个制品**（`winlogbeat-8.19.0-windows-x86_64.zip`），manifest 声明的另外两个（Linux/Windows Filebeat）不在缓存里，`dist/` 下也没有 unified 目录包产物。**顺序上必须先有授权结论，再决定能否下载并缓存这两个制品**——这正是两个缺口关联的地方。
    - 兼容性一项其实有实践证据但未成文：链路在 Kafka 4.3.1 上以 `version: "2.1.0"` 持续投递（Zeek raw 今日 10 万+、tenant_a 23,075），磁盘队列目录在四个实例上都在活动。**受管运行**若指 Management Agent 下发，则属 `COL-08`/D1.5。
- [x] COL-03（G/D，P0，依赖 COL-01）交付 tuba-source-adapter：按服务端 Topic→来源 context 绑定解析 Beat 原文，经 ingest 获取与来源 context、topic/partition/offset 和正文 hash 匹配的 receipt 后提交 offset；固定位置/正文重试，只有本地确认的永久输入错误先持久 DLQ。已有 `cmd/tuba-source-adapter` 与 `internal/sourceadapter` Kafka→内部 HTTP receipt→手动提交/DLQ 骨架；本地配置只含 Topic allowlist，通过 `SOURCE_ADAPTER_TOKEN` 认证，所有 HTTP 错误均保留 offset，避免授权或映射故障造成跳数。默认 loopback `/metrics` 已提供读取、接收、拒绝、重试、DLQ、offset commit 计数器。当前只支持 Filebeat/Winlogbeat 事件，非 Beat 连接器尚无入站合同。尚无 Management Agent 配置下发、token 发放/轮换、Kafka ACL 编排、仪表盘/告警和端到端运行验收。验收：伪造租户、崩溃重读、重复发送、超大消息和上下文换代行为符合合同。
  - **2026-09-30 复核：验收的五个场景已全部覆盖，本项勾选。** 其中三个原本就有测试（伪造租户：`internal/ingest/server_test.go` 注入 `"organization":{"id":"attacker-controlled"}`，断言信封采用**服务端解析**的身份；崩溃重读：`TestRunRetriesKafkaFetchAndCommitWithoutRedeliveringAcceptedRecord`；重复发送：两个 receipt 去重测试 + 现场实测），另外两个只有实现没有测试，本轮补齐：
    - **超大消息**：新增 `TestBeatIngressRejectsAnOversizedPayload`，投递超过 1 MiB（`rawevent.MaxPayloadBytes`）的**格式合法**正文，断言 413 且**不产生任何 Kafka 写入**——截断成一次成功等于悄悄改了证据。
    - **上下文换代**：新增 `TestGroupIDSeparatesSourceContextGenerations`，断言不同 source context 与同一 context 的不同 topic 版本都落到**不同的 consumer group**；否则重建后的适配器会从退役代次停下的位置继续，把它从未读过的记录静默跳过。
    - 两条新测试都做了**变异验证**（把组 ID 改成与 topic 无关、把 beat 路由的上限放开 8 倍），确认它们真的会失败，不是恒过的装饰。
    - 回归：`go test ./...` exit 0、**26 个包 ok、0 FAIL**，`go vet ./...` exit 0 无输出（按此前教训写文件后数 FAIL，不用 `| head` 以免 SIGPIPE 把空输出误读成通过）。
  - **本项文字里“尚无”的几项归属**：Management Agent 配置下发 → `COL-08`，按 D1 定义属 D1.5；Kafka ACL 编排与仪表盘/告警已完成（`C05`/`O05`）；端到端运行验收已达成（tenant_a raw 23,075 / UIM 23,102，Zeek raw 今日 10 万+）。**唯一仍属本项范围的遗留是 adapter token 的发放与轮换没有自动化**——目前是两端静态环境变量加重启。验收标准未要求它，故不阻塞本项勾选，但在此写明。
- [x] COL-04（G/O，P0，依赖 COL-02/03）交付 Zeek Filebeat 接入包，在 21 新建独立 TUBA 实例，覆盖 conn/dns/http/ssl、活动日志、最近 90 分钟 gzip 归档、原文与位置。`scripts/manage_zeek_filebeat.py` 管理四个独立 Filebeat、registry/磁盘队列和每 30 秒归档解压子进程；每路有专属 context/Topic/凭据。21 的 Filebeat 8.19.0 不解压 gzip，已改为先在 TUBA 私有 spool 校验解压再读取。真实数据通过 248 隔离 Kafka→source-adapter/receipt→Raw→DIP/UIM→network/dns/web/tls ES alias。19:47 快照：四个 source Topic 消费组均 lag=0；Raw Topic end offset 51,270；Normalizer 与 Raw indexer lag=0；标准 indexer 仅 network 流在途 2 条，其余领域 lag=0。Raw alias 51,665 条（含更早隔离样例）；domain alias：network 33,012、dns 10,048、web 139、tls 371；quarantine alias 8,140；本轮 Raw-indexer DLQ 无新增（独立 DLQ Topic 留有早期旧轮次 423 条）。首轮 gzip 压缩字节误入 Raw 的 4,295 条以 `DIP_ZEEK_ORIGINAL_INVALID` 隔离并保留；解压修正后该原因未再增长。无 SNI TLS、无 query DNS、无 host HTTP 按当前合同隔离。既有 systemd Filebeat 保持 active，未改动。归档 spool 清理策略、容量/保留及长期恢复演练仍由 COL-07/A03 跟踪。
- [ ] COL-05（G/O，P0，依赖 COL-02/03）交付 Winlogbeat Security 接入包，配置 Event ID、读取权限、原生 XML、频道/主机/RecordID/代次、bookmark 和清空覆盖告警。验收：139/169 按确认范围闭环；不再开发自研 Windows Event Log adapter。
  - 2026-09-29 开发与端点 shadow 验证：锁定官方 Winlogbeat 8.19.0 amd64，官方 SHA-512 核验；`deploy/components/windows-security-winlogbeat.example.yml` 使用每主机独立 registry/disk queue、Security 白名单和 `include_xml:true`。新增稳定 Winlogbeat source position 与 1102 清日志、4672 特权、4719 审计策略、4723/4724 密码操作 UIM 语义；go test 全量通过。139/169 使用各自 Administrator 只读运行 24 小时 shadow（输出仅保留于独立临时目录，采集后清理），均成功读取并生成带 XML 的事件；139 1762 条（4719 1728、4776 34），169 111 条（4776 111）。未写 Kafka或注册服务。正式采集尚未启用：必须先经现有授权 API 建立两条 Windows source instance/context 和 active/staged release，再分别创建源 SCRAM 身份、精确 Topic ACL，并把 Topic 加入 248 adapter allowlist。当前会话没有有效的 TUBA `source:manage` 操作者令牌；未绕过应用授权用 SQL 伪造身份/审计，也未向两台主机下发 Kafka 凭据，因此 COL-05 保持开放。

  - **2026-09-30 复核：上一条子项已过时。** 它记的是“正式采集尚未启用”，但同日 19:48 已通过授权 API 建立两条 Windows source instance 并启用，链路当天即闭环。**实时证据**（2026-09-30 经 21 读 248 的 ES）：tenant_a raw 分区 09-29 18,196 条 + 09-30 4,879 条（合计 23,075）；UIM 四领域 authentication 11,239、session 8,995、iam 2,856、network 12（合计 23,102）；两条来源均 enabled。
  - **已具备的交付物**：官方 Winlogbeat 8.19.0（SHA-512 核验）；24 个 Event ID 白名单（含 1102 清日志）；`include_xml: true`；每主机独立 registry/disk queue/context/凭据；`registry_flush: 1s`；稳定 Winlogbeat source position（computer/channel/RecordID/事件时间，含“按事件时间区分日志代次”的单测）；`<RenderingInfo>` 剥离与易变字段 `drop_fields`（payload 字节稳定性）；`setup.template/ilm.enabled: false`。**不再开发自研 Windows Event Log adapter** ✔——Go 侧只有 Winlogbeat 事件字段的解析与位置计算，没有自研读取器。
  - **仍然不能勾选的两条理由**：
    1. **“清空覆盖告警”没有落地。** Event ID 1102 已在白名单里被**采集**，但全仓库没有任何针对它的告警规则（`deploy/observability/` 无命中），也没有“安全日志循环覆盖导致记录丢失”的缺口检测——而 `COLLECTOR-DESIGN.md` 把“清空/覆盖缺口”列为该连接器的设计要求。**采集到证据不等于有告警**，本项的字面要求未满足。
    2. **“读取权限”只在过程中被证明，未固化为交付物。** 24 小时 shadow 用各机 Administrator 成功读取，说明权限够用；但接入包模板没有写明所需账号/权限（如 Event Log Readers 与管理员），也没有脚本或部署前置校验。属“事实上满足、契约上未固化”。
- [ ] COL-06（G/O，P0）实现来源范围配置与 TUBA 过滤策略版本、影子计数、原因码、场景保护及回滚；区分源端过滤与 adapter 准入过滤。验收：无审计计数的源端复杂规则不能发布，平台过滤不能冒称减少源端网络流量。
- [ ] COL-07a（G/O，P0）非破坏性可靠性验收（D1；2026-09-30 由 COL-07 拆分）：采集端强杀后重读与补齐、日志轮转不丢记录、重复 offset/跨 offset 重发、归档 spool 回放与确认水位、采集端断网后补齐。**已完成前四项**，逐项证据见下文 2026-09-30 的四条记录（方法、观测量、结论俱全）。**唯一未做的是断网**：可在 21 上以 iptables 精确限制到 Kafka 端口、秒级回滚，属可做范围，但尚未执行。因此**本项暂不勾选**——四项完成不等于本项列出的场景全部覆盖。
- [ ] COL-07b（G/O，P0）破坏性故障注入（D4；原 COL-07 的破坏性子集）：磁盘满、Kafka/PG 故障、Topic 重建、目标机重启、积压期间换凭据与配置。**阻塞于两件事**：(1) 根盘常态 71%、分配线 75%，只剩 4 个点余量，而积压类测试恰好会推高占用，会重演 2026-09-30 的只读故障——需要先加盘扩容；(2) **目标机重启按用户 2026-09-30 决定暂不做**：单节点没有远程带外恢复手段，若重启后组件未自动拉起，平台会停在停机状态且无法远程干预。2026-09-30 由 COL-07 拆分而来，拆分理由：非破坏性子集已完成（见 COL-07a 的证据），而破坏性子集未做，混在一个编号下会让 D1 永远关不掉这一条。
  - **2026-09-30 状态：非破坏性子集已完成，破坏性子集未做，因此本项不整项勾选。** 完成的四项非破坏性验收（重复投递与跨 offset 重发、采集端强杀后重读与补齐、日志轮转不丢记录、归档 spool 回放与确认水位）已逐项留证据，见下文 2026-09-30 的四条记录。**未做的是破坏性子集**（磁盘满、Kafka/PG 故障、Topic 重建、目标机重启、积压期间换凭据与配置），它依赖加盘扩容——当前根盘常态 71%、分配线 75%，只剩 4 个点余量，而积压类测试恰好会推高占用，会重演 2026-09-30 的只读故障。按 D1 定义，D1 计入的是本项的**非破坏性子集**（四项已完成，断网一项未做），破坏性子集归 D4。**拆分已执行（2026-09-30）**：本编号不再作为待决策项存在，见上方 COL-07a／COL-07b；下文各条日期记录保留为两者的共同证据。
  - 已完成其中的单节点 worker 故障切片：PostgreSQL/Kafka 中断恢复、Launcher 托管进程强杀重启和优雅退出均于 2026-09-28 在 71 隔离通过，详见本 TODO 的 O04/COL-07 演练记录。其余 COL-07 场景及 Zeek Collector spool 端到端积压/重复投递演练仍待完成。
  - 2026-09-28 增加本机回归：source-adapter 注入 Kafka Fetch 与 offset commit 短暂失败，验证 retry 后 ACK/offset 顺序保持且只调用一次 ingest receipt；ingest 增加来源 Topic 绑定、可信上下文、重复 receipt 幂等、正文冲突、无效 token 和未绑定 Topic 覆盖。`go test ./...` 与 `go vet ./...` 均通过。248 上六个 Zeek worker 全部由监督器管理，source-adapter 子进程 SIGKILL 后 supervisor 拉起新 PID、readiness 恢复；现场只读复核六进程 running、Prometheus 6/6 targets UP、Kafka lag 0。此证据关闭 worker 单进程崩溃恢复切片，不能替代整机重启、断网、磁盘满、轮转覆盖、Kafka/PG 故障矩阵与 spool 回放验收；详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。
  - 2026-09-29 已把 Filebeat registry 解析接入 `archive-sync`：稳定读取 active snapshot 与 WAL 的 set/remove，cursor 到达 stage 文件 EOF 才列入可回收集合；registry 缺失、格式异常或读取期间变化均 fail-closed。6 项本地 Python 测试和 py_compile 通过。21 上以临时副本只读核对后部署并备份旧脚本，重启的仅为 `archive-sync` 子进程，四个 Filebeat 不间断；归档状态读回证明已确认文件被回收，而未确认文件仍保留。每 30 秒继续执行同一安全规则。详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。
  - 2026-09-29 在 21 完成三轮隔离队列测试：2,000 条输出不可达、约 1,919,784 B 持久化后 SIGKILL，恢复后 unique=2,000、duplicates=0；50,000 条将 16 MiB queue 填至 15,999,234 B 后强杀，恢复后 unique=50,000、duplicates=0，未入队数据从输入文件续读。真实 broker 停止/启动轮发送 100,000 条，queue 达 15,999,360 B 后强杀 Filebeat 并恢复同一 Kafka/队列；三次实测均完整收到 100,000 个唯一事件、malformed=0，但重复分别为 15、94、65 条。重复记录的 Filebeat `log.file.device_id`、`log.file.inode`、`log.offset`、正文 hash 和 agent 元数据相同，Kafka delivery offset 不同，证实该故障窗口提供 at-least-once 语义。
  - 2026-09-29 实现并部署 Beat 稳定位置：Filebeat `filebeat-v1:<device_id>:<inode>:<log.offset>`；Winlogbeat `winlogbeat-v1:<hex(computer)>:<hex(channel)>:<record_id>:<UTC timestamp>`。Kafka `topic/partition/offset` 单独作为投递位置；Windows Security 缺稳定位置时拒绝接收。旧 Raw 日索引使用 `dynamic:strict` 且映射不含可选 `delivery_position`，因此继续把投递元数据持久化在 PG receipt 和 Raw Kafka 信封，Raw ES serializer 不写该字段，避免对历史索引做管理员 mapping 迁移。新 `tuba-ingest`、`tuba-source-adapter`、`tuba-raw-indexer`、`tuba-normalizer` 已部署至 248，旧二进制备份于 `/opt/tuba/backups/reliability-20260929/`；原 context、服务 token 与 group suffix 保留，offset 未重置。Go 全量测试、`go vet ./...`、Python 管理器 6 项测试及 py_compile 通过，Winlogbeat 8.19.0 配置通过 `test config`。需要补真实跨 offset Kafka→receipt→Raw ES→DIP/UIM ES 重放验收后才能关闭 COL-03/COL-07。
- [ ] COL-08（G，P0）复用 enrollment、30 秒心跳、ETag 轮询，实现 TUBA Management Agent；报告 Beat 版本、运行状态、期望/生效配置和队列指标，离线保留有效配置。验收：管理与采集状态分别呈现，禁用能撤销来源 Kafka 写权限。
- [ ] COL-09（G/O，P1）将现有 CLI 改为受管组件监督器：单实例锁、独立 registry/data、进程身份、优雅停止、崩溃退避、签名组件升级、状态格式兼容检查和回滚。验收：跨 OS 相同命令，不注册 systemd/Windows Service；主机重启恢复方式明确。
- [ ] COL-10（G/W，P1）提供来源/组件/版本/健康/过滤/缺口管理 UI 和审计；模板生成配置，禁止任意命令/YAML/明文凭据，分阶段展示接入确认与索引新鲜度。
- [ ] COL-11（G/O，P1）交付 Syslog 网关接入包，优先 TCP，生产 TLS 独立配置；持久文件或队列、RFC/framing/时区/设备绑定/限额与原文。验收：UDP 明确尽力交付，网关重启及轮转可恢复，不信任正文 hostname 作为租户授权。
- [ ] COL-12（G/O，P1）核对 JumpServer 实际版本和审计接口，交付文件/Syslog＋必要 API 连接器；登录/资产访问/命令审计逐项标明覆盖，录像/文件证据仅存受控引用。
- [ ] COL-13（G/O，P1）核对 Keycloak 版本，配置用户与管理员事件及 listener，交付 Filebeat 或 API 接入包；验收登录成功/失败/登出/管理操作，不把服务运行日志当完整审计。
- [ ] COL-14（G，P1）建立专用连接器框架：API 分页/限流/重叠补采/持久游标、Webhook 验证/持久确认、DB 增量边界；以一个真实来源验证幂等与重启恢复。
- [ ] COL-15（G/O，P0）迁移旧自研路径：冻结 reader/spool/Sender 新功能，盘点未确认队列、排空并记录切换水位；新方案隔离验收后退役旧进程，保留历史证据和回滚窗口。验收：同来源不向同一生产代次双写；不影响其他链路。2026-09-30 现场处置：21 上被取代的 Zeek 采集代 r1（`collector-live/filebeat/config/*`，4 进程）自 2026-09-27 起持续以 401 失败——其 `source_instances` 已 disabled、写 ACL 已撤销，累计 **49,144 条 `not authorized`**（conn 13,276 / dns 12,655 / http 10,660 / ssl 12,553），日志约 71 MB 且持续增长；它正确地不推进 registry 并无限重试，即永远不可能成功。已按 PID 精确 SIGTERM 停用，约 2 秒优雅退出，**registry 原样保留**作为回滚位置（`collector-live/filebeat/data/*/registry`，冻结于 2026-09-27 18:13）。活跃代 r2 与别的产品的 `filebeat.service`（systemd，pid 2458981）均未受影响：停后 r2 源 topic 仍 +122/30s、Zeek raw 文档 +313/30s。**顺带发现两处待修**：(1) `collector-live/filebeat/manage_zeek_filebeat.py` 的 status/stop 依赖 `run/` 下的状态文件，该文件已丢失，于是 status 误报 `stopped`、stop 空转——停用必须按 PID 核对 cmdline 后执行；(2) 日志里 `"file.line":401` 会被宽泛的 `401` 正则误计为授权错误，统计授权错误只应匹配 `not authorized`。

执行顺序以“当前阶段与 Zeek 闭环后续顺序”为准；COL-01/02/03 持续补齐，COL-04 已完成真实 Zeek 接入，COL-05 扩展 Windows 来源，COL-06/07 完成过滤与可靠性，COL-08/09 支撑统一部署，COL-11–14 扩展来源，最后按 COL-15 完成来源切换。未勾选条目仍待实现或验收，文档改版不视为采集程序已替换。

## 阶段 4：DIP/UIM 与多领域索引（P0；依赖阶段 3、C02、C05、C07、C09）

- [ ] N01（G）创建 tuba-normalizer，加载不可变发布包、版本缓存、来源绑定和 Kafka 事务；实现 fencing/read_committed。基础单进程 raw→UIM→Kafka worker 已存在；发布包缓存、事务与 fencing 尚未完成。
- [ ] N02（G）迁移 Windows Security DIP，覆盖 authentication/session/iam/directory，按事件语义映射 user/user.target/group。
- [ ] N03（G）迁移 Zeek DIP，覆盖 network/dns/web/tls，保留来源语义及原生字段，不虚构身份。首片支持 Zeek conn/dns/http/ssl，覆盖 endpoint、端口、transport、计数器、DNS 查询、HTTP 和 TLS 字段；其它 dataset/字段覆盖与发布规则绑定待完成。
- [ ] N04（G）实现 UIM 来源合同、受控分类、类型/枚举/最低语义、质量/能力及路由；不信任源端 quality/route。初版 `uim.Normalize/Validate` 已生成可信 route/provenance 和 qualified/partial 状态；完整 Schema/发布包校验、能力合同与逐领域枚举仍待完成。
- [ ] N05（G/D）实现 Quarantine 合同及索引，保存原始引用、阶段、规则版本、失败字段及原因；单条异常不阻塞分区。
- [x] N06（G/D）扩展 indexer 支持八领域与固定日期索引；实现等价 409、内容冲突隔离、逐条 bulk 结果和连续 offset。标准 indexer 使用有界批量读取、ES bulk、逐条可重试/永久错误分类，复用 `INDEX_BATCH_SIZE`、`INDEX_BATCH_WAIT`；测试覆盖逐条重试、内容冲突转 DLQ、scope 隔离和 DLQ 失败时不提交 offset。2026-09-27 隔离实测覆盖八个领域、UTC 日期物理索引、同 ID 异内容 DLQ 与全部输入 offset 追平，详见本节验收记录。
- [ ] N07（G）区分暂时重试与永久 DLQ；DLQ 不可用时不提交输入；过期数据不自动重建已删除索引。
- [ ] N08（G/O）在隔离 namespace 对照旧 ES Pipeline 输出，记录差异并修正规则；生产来源切换时只激活一条路径。

阶段出口：真实 Windows/Zeek 原始样例经过同一可信链路进入八领域，标准消息可供索引与分析共同消费。

## 阶段 5：任务与一致性基础（P0；依赖 C06、C09、阶段 2）

- [ ] T01（G）创建 control-worker：任务状态机、attempt、租约、fencing、重试、取消、心跳与 SKIP LOCKED 领取。
- [ ] T02（G/P/D）实现 inbox＋状态＋checkpoint＋outbox 单事务协议，恢复以 PG checkpoint 为权威；处理 rebalance/旧租约写入。
- [ ] T03（G）实现有界 outbox publisher、聚合键顺序、投递重试和超时告警；无业务状态跨进程共享文件。
- [ ] T04（G）受权限控制、可审计的 release 发布闭环。代码/API/合同和手册已交付；248 的 00012/00013 迁移、首位 publisher 引导和新版 API 部署已完成，Windows Security bundle 已就位。剩余：用有效操作员会话登记 manifest，执行 draft→validated→staged→active，并读回 actor/action/request ID 审计后勾选。release 资产不可原位覆盖；source 必须显式绑定 staged/active release。
- [ ] T05（G/D）实现 inbox/outbox/state retention、任务临时文件清理、活跃租约/证据保留保护。

## 阶段 6：实体与归因（P1；依赖阶段 4、阶段 5、C03）

- [ ] E01（G/D）实现 Account/Device 身份空间注册、强弱标识优先级、规范化和 entity.id 生成。
- [ ] E02（G）实现多角色 attribution，包含 resolved/unresolved/ambiguous 和证据；event.id 不被改写。
- [ ] E03（G/D）实现时态关系、有效区间、规则快照与冲突处理；缺少关系不阻止单实体特征。
- [ ] E04（G）输出按 entity.id 分区的 attributed 消息，多角色具有独立贡献键；支持事件级未归因检测输入。
- [ ] E05（G/D）扩展 sink 写实体/归因/关系投影，revision 防止旧值覆盖；实体来源/历史可追溯。

阶段出口：同名跨域/跨租户账号不合并；同事件多角色可解释；重启和重复消息不重复生成关系贡献。

## 阶段 7：特征、基线与检测（P1；依赖阶段 6、T02–T04、C04）

- [ ] F01（P）拆分 analysis-worker 内的 feature/baseline/detection 模块；统一 registry、质量门槛及依赖版本。
- [ ] F02（P/D）实现有界窗口状态、实体/角色输入去重、watermark、idle 分区及未来时间保护。
- [ ] F03（P）实现至少认证计数/失败率/失败后成功等特征；覆盖迟到修正、窗口关闭、超界回填。
- [ ] F04（P/G）实现基线训练任务、截止时间、最小样本、cold_start、评估和不可变版本发布。
- [ ] F05（P）交付失败后成功、失败聚集规则和一个统计基线检测场景；输出解释、证据、特征及模型版本。
- [ ] F06（P/G）实现相同业务键 revision 更新和 retracted；规则升级通过 generation 隔离，不产生双份线上告警。
- [ ] F07（G/D）扩展 analysis-sink 多对象写入、外部 revision、同 revision 内容冲突、固定日期键及 DLQ。
- [ ] F08（G/P）统一正式调度入口，Go 认证 CLI 标记为诊断用途，禁止双实现重复产出。

## 阶段 8：风险、案件与反馈（P1；依赖阶段 7）

- [ ] R01（P/D）实现不可变风险贡献、异常修订补偿、实体聚合、关联去重和定时衰减。
- [ ] R02（G/D）保存当前风险及解释投影，固定计算版本和更新时间；撤回异常后风险正确回退。
- [ ] R03（G）保留 PG 案件权威，补齐实体/风险/证据关联、取证快照/保留标识、版本并发控制和幂等。
- [ ] R04（G/P）反馈进入审计与离线评估数据集；不直接改生产模型；自动建案如启用需独立去重合同。

## 阶段 9：查询、API 与控制台（P1；依赖 C08，分模块依赖阶段 4–8）

- [ ] Q01（G）实现 Data Model/Dataset/Query Catalog，强制 tenant/namespace/active generation 和质量条件。受限 `GET /api/v1/events` 已实现领域、租户、时间范围和分页查询，Data Model/Dataset Catalog 与质量筛选仍待实现。
- [ ] Q02（G）实现文档列明的 SPL 子集与逻辑计划，字段白名单、分页/聚合限制、超时和拒绝任意 DSL。
- [ ] Q03（G）实现异步导出、下载重新授权、敏感字段/原文权限、过期链接及审计。
- [ ] W01（W/G）来源/DIP、UIM 质量、隔离详情、版本发布、任务及回放管理页面。
- [ ] W02（W/G）事件查询、原始引用、血缘、归档中/已过期/索引滞后状态。
- [ ] W03（W/G）Account/Device 详情、多角色/关系、特征/基线、异常与风险解释。
- [ ] W04（W/G）完善案件/反馈、权限管理、审计、运行总览和备份状态。
- [ ] W05（W/G）核对 UI 状态、API 错误与权限一致，所有演示假数据明确移除或隔离。

## 阶段 10：回放、升级与恢复（P0 运维闭环；依赖 T01–T05、阶段 4、阶段 7）

- [ ] B01（G/P）实现按来源/时间/release/generation 的回放和回填任务，固定输入范围与输出状态空间。
- [ ] B02（G/D）实现影子 generation、追平水位、PG 激活指针、查询切换及回滚补处理，防止新旧重复进入风险。
- [ ] B03（O/G）交付单节点 install/upgrade/rollback/uninstall 操作手册；卸载默认保留数据，迁移采用 expand/migrate/contract。
- [ ] B04（O/D）配置 PG 基础备份/WAL、ES snapshot、身份/发布包备份到异机，记录恢复清单及水位。
- [ ] B05（G/O）实现 checkpoint 超过 Kafka retention 的缺口检测与恢复任务，不自动跳 latest。
- [ ] B06（O）整理容量、磁盘压力、Kafka/ES/PG/OIDC 故障、消息积压、隔离修复和密钥轮转 Runbook。

## 阶段 11：实施后的验收任务（P0/P1；依赖对应实现）

- [ ] V01 合同与真实样例：八领域正常/缺字段/坏格式/不支持类型/多角色/来源重置均有输入和期望结果。
- [ ] V02 接入可靠性：Kafka 故障、Collector 断线、重复提交、重启、限流及原始归档积压不丢已确认记录。
- [ ] V03 一致性故障点：Kafka 事务中断、PG 提交前后崩溃、outbox 重复、sink 部分成功、rebalance/fencing 恢复正确。
- [ ] V04 索引幂等：跨日期重试、同 ID 内容冲突、revision 乱序、过期索引、generation 切换不产生重复或旧值回退。
- [ ] V05 实体/分析：跨身份空间同名、弱身份、unresolved、迟到/未来时间、冷启动、撤回与风险补偿符合合同。
- [ ] V06 安全：列表/详情/聚合/导出/原文/任务/回放跨租户隔离，失效成员、服务 ACL、凭据及诊断信息无泄漏。
- [ ] V07 单节点容量：在 A03 约定负载下记录 EPS、端到端 P95、新鲜度、积压恢复速率、内存/磁盘/PG 事务预算。
- [ ] V08 灾备与升级：从异机备份恢复，核对 Raw/标准/派生/案件水位，记录实测 RPO/RTO；完成升级与回滚演练。
- [ ] V09 产品闭环：Windows 与 Zeek 接入→标准检索→实体→特征/基线→异常/风险→建案/反馈→受控回放完整演示。
- [ ] V10 文档收口：合同、配置、迁移、代码、UI 与目标一致；每项勾选附证据，更新 product README 的真实交付状态。

### 2026-09-29 本轮可靠性与 Windows 接入状态

- 248 O05 监控、容量保护、磁盘水位与消息保留已部署且复核健康；邮件外发按用户要求暂缓。
- 21 Filebeat archive-sync 改为读取 Filebeat registry 快照/WAL，只有稳定游标达到文件 EOF 才清理 stage；无法证明的过期文件 fail-closed 保留。现场首轮只回收 36 个已确认文件，保留 108 个待确认文件（约 16.8 MB）；四个 Filebeat 进程未重启。
- 248 已部署稳定源位置和重复 receipt 兼容版本，四个更新组件 hash 与构建产物一致、旧二进制已备份。六组件 running、ingest readiness 200、scrape targets 6/6 UP；两轮各 5 次、间隔 30 秒的 lag 样本在 0–10 间波动，最终读数均为 0。短时非零分布在 network、DNS 标准索引以及 conn source-adapter/Raw indexer，没有呈现持续单向增长；仍需长期观察 lag 和索引新鲜度，不能以一次归零替代稳定性窗口。
- Winlogbeat 采集器和 UIM 语义已通过包配置/Windows shadow，但未进入 Kafka/TUBA。此前缺少有效 `source:manage` 会话；2026-09-29 已按用户明确请求，在 247 创建 `tuba-operator-20260929`，并通过受限的一次性引导命令为实际 Keycloak issuer/subject 在 `tenant_a` 建立 `tenant_admin` membership，审计 action=`identity.bootstrap_membership`、actor 为空并在 metadata 记录授权来源。登录 token 经 248 `/api/v1/me` 读回 `source:manage`/`user:manage`，`GET /api/v1/sources` 返回 200。原先的 local-dev `tenant_admin` membership 使用不同 issuer，不可登录，不计为当前 realm 的有效管理员。
- 2026-09-29 当前 TUBA `source_instances` 列表为空。248 `release_bundles` 原有 `zeek-validation-20260927-v1` staged 记录的 manifest 是空对象 `{}`，不能作为 Windows Security 语义 release；本轮没有改动该旧记录。Windows Security bundle 已补齐并预置 248；T04 API、迁移及 publisher 引导已部署，实际 release row 仍待有效操作员登录后按审计流程创建。发布完成后，再通过来源 API 分别登记 139/169，创建独立 Kafka SCRAM 身份/精确 ACL、更新 adapter allowlist，最后完成 Kafka→receipt→Raw ES→DIP/UIM→标准 ES 的逐段数量及重放验收。
- 2026-09-29 登录与 Winlogbeat release 续进：当前操作员曾通过 `/api/v1/me` 验证 `tenant_admin`、`source:manage`、`user:manage`，来源列表 API 返回空数组。新增 `releases/windows-security-1.0.0/`，包含 DIP、Windows 事件到 UIM 语义/四域 routing、authentication/session/iam/directory ES templates、实体/数据模型和显式 ingestion-only 空分析规则；release manifest 七类资产/依赖/哈希通过 `scripts/validate_release_bundle.py`，canonical digest=`0195fd13e09ff09684af16827c0156a457af4e7a4fdcc1655b4939efa6355a74`。release 发布 API 已实现且在 248 部署，bundle 7 个 asset 的服务端文件 hash 验证已完成；当前因浏览器 access token 被 API 判为无效，尚未创建/推进 release row。
- 尚不能宣称“全部可靠性任务完成”：COL-07/V02 的 Topic 重建、离线 queue 恢复接入、磁盘满/系统重启、源日志覆盖、积压期间凭据切换、跨 offset 实流重放与逐段数量对账未全验；B04/V08 异机备份目标与恢复演练未定；O05 外发邮件暂缓。未重置 Kafka offsets 或清理未确认数据。本轮按用户授权为当前 operator subject 新增首位 global release publisher，并记录 bootstrap 审计；未做其他权限变更。

### 2026-09-30 已知数据缺口：隔离（DLQ）事件

Windows 接入与位置/哈希稳定性排查结束后，按用户决定将这些隔离事件**记录为已知缺口，不做回放**。缺口不等于“数据丢失”：事件正文完整保存在 DLQ topic 内、可从中恢复或重放，但它们**不在任何 ES 索引里**，对查询不可见，也不能计为已交付数据。

两个 DLQ topic 均为 `retention.ms=86400000`，即**滚动保留 24 小时**：

| Topic | 记录时数量 | 主要失败原因 |
| --- | --- | --- |
| `tuba.collector.zeek_validation_20260927_001.dlq.v1` | 9,565 | 早期 `raw envelope contract or tenant scope mismatch`（旧 ingest 把 Windows 信封写进 Zeek raw topic）与 `quarantine identity or tenant scope is invalid`；末段为 `payload_hash mismatch`（2026-09-30 约 11 分钟部署不完整回归，见下） |
| `tuba.collector.zeek_validation_20260927_001.source-adapter.dlq.v1` | 2,605 | `INGEST_REJECTED_409`：同一 source position 配到不同 payload |

409 的成因经逐字段取证确认为三类，均已修复：

1. **Windows 渲染不稳定**：同一记录两次读取时，`event.original` 内 `<RenderingInfo>` 的本地化任务名不同（实测 `Logon` vs `Credential Validation`，为整份文件中唯一差异）。已由 Winlogbeat `script` processor 剥离 `<RenderingInfo>`、并 `drop_fields` 移除 `agent.ephemeral_id`/`event.created`/`event.action`/`winlog.task|opcode|keywords` 修复；`<System>` 与 `<EventData>` 原始数据保留。
2. **Beats JSON 字段顺序随机**：同一内容两次投递的字节序不同（实测**规范化后 sha256 完全相同、0 字段差异**，而原始字节长度 5647 vs 7264）。已由 `rawevent.CanonicalPayloadHash`（排序键 + `json.Number`）修复，ingest、adapter、`rawevent.New/Validate` 三处统一。
3. **同一 Zeek 记录被从两个路径读入**：Filebeat 同时读活日志与归档目录，而 Zeek 轮转是把 `conn.log` **复制**进归档，inode 随之改变。实测同一记录的两次投递唯一差异是 `log.file.inode`（1592008 vs 3941775）与 `log.file.path`（活路径 vs `archive/conn/conn.01:00:00-02:00:00.log`），其余 37 个字段完全一致且 `uid`/`ts` 相同——**是同一记录的重复投递，不是不同记录撞位**。
   修复分两步，缺一不可：
   - `StableBeatPosition` 优先使用 `log.file.fingerprint`（新形式 `filebeat-v2:`）配合 Filebeat `file_identity.fingerprint`，使两条路径产生**相同**位置。这一步把问题从“同一事件被索引两遍”变为“位置正确、payload 因位置元数据而不同”。
   - `CanonicalPayloadHash` 将采集器传输元数据（`log.file.path`/`inode`/`device_id`、`agent.ephemeral_id`、`event.created`）**排除在摘要之外**，字段本身仍完整保留在存储的 payload 中。这是为满足 `contracts/events/beat-ingress/1/contract.md` 对保留 `log.file.path` 的要求——不能靠丢弃字段解决。
   验证：修复后 150 秒内 adapter DLQ **零新增**（修复前约 1 条/秒）。

   订正说明：本节此前把第 3 类根因记为“inode 复用导致不同记录撞位”，并据此推断 `StableBeatPosition` 未生效。**该推断已被实测推翻**——位置始终稳定（两次投递位置完全相同、`uid`/`ts` 相同），真正原因是同一记录携带了随路径变化的元数据。保留订正过程，以免读者被早先的错误结论误导。

另记录一次本轮自身造成的故障，不做隐瞒：2026-09-30 部署规范化哈希时只更新了**计算方**（ingest、adapter），遗漏了同样调用 `rawevent.Validate` 的**校验方**（raw-indexer、normalizer），导致约 11 分钟内每条原始信封都因 `payload_hash mismatch` 进 DLQ，raw-indexer 吞吐由约 250 条/秒降至约 1 条/秒。补齐这两个组件并重启两条链后恢复。教训：**改动共享校验函数的语义时，必须同时找出所有调用方**，不能只顺着“谁计算”去找。

**出口期限**：DLQ topic 滚动删除，上表计数会持续下降，**更早的内容已经不可恢复**。若要将其作为证据留存或回放，必须在各自的 24 小时窗口内导出；逾期则本缺口应视为“永久未索引”，只保留本节的计数记录。

**未做的验收**：修复后未做端到端回放验收（重放 DLQ 内容并核对 ES 数量）。按用户决定，本轮只记录缺口、不回放。

### 2026-09-30 容量实测：ingest_receipts 是根盘增长的主因

根盘水位由 68% 升至 72%（越过 A03 的 70% warning 水位）后逐层排查，结论如下。

**构成**：根盘 32G/44G。Kafka 6.0G（raw topic 2.4G、events.network 1.4G）、Elasticsearch 4.8G（在 `/var/lib`，不在 `/opt/tuba`）、PostgreSQL 其余表均 ≤ 96 kB。**唯一在无限增长的是 `ingest_receipts`**：

| 项 | 值 |
| --- | --- |
| 记录时表大小 | 2,511 MB（1,749,621 行，覆盖 09-27 16:14 起全部历史） |
| 每行占用 | 约 1,506 字节（`trusted_metadata` 存了信封元数据快照） |
| 写入速率 | 4.1 行/秒 → 约 0.50 GB/天 |
| 判定的期限 | 约 3 天到 75% critical、7.4 天到 80% 停止写入 |

Kafka 与 Elasticsearch 各自有保留期、会封顶；**receipt 表每个事件一行、从不清理**，这才是 A03 预算没有覆盖的那一项（A03 只算了 Kafka + ES）。

**已执行的清理**（`scripts/prune_ingest_receipts.sh`，保留 2 天）：

- 删除 243,236 行，表 2,511 MB → 1,960 MB；`VACUUM FULL` 回收 551 MB，用时 40 秒
- 根盘回到 30G/44G（69%），**已低于 70% warning 水位，增长停止**
- 保留期取 2 天的依据：可重投窗口由 Kafka 保留期（24 小时）与采集端 `ignore_older`（Winlogbeat 24 小时）界定，2 天是其两倍余量。**不要在不看数据的情况下放大窗口**——receipt 只从 09-27 开始累积，7 天窗口一行都清不掉，等它生效时磁盘早已填满

**本次清理自身的两个缺陷（已修）**：

1. 初版默认保留期拍定为 7 天，未先核对数据分布，实际清不到任何行。改为 2 天，并把依据写入脚本注释。
2. 初版截止点写作 `now() - interval 'N days'`，**每批重新求值**，新数据持续“老化越界”，批次数永远不归零、循环不终止（首次执行时计数由 229,822 涨至 247,601 仍未退出，被迫中止）。改为运行开始算一次固定时间戳，并加迭代上限作安全阀——该安全阀在修复前的第二次运行中正确触发并报错退出，而非静默跑下去。

**遗留观察项：zeek 适配器有约 11,700 条常驻积压（约 49 分钟数据）。** `VACUUM FULL` 阻塞接入 40 秒期间适配器落后，此后其消费速率（约 3 条/秒）与 conn 源 topic 的生产速率（约 4 条/秒）基本持平、**无法反超**（适配器逐条处理，每条一次 HTTP 加一次同步 offset 提交），积压缓慢增长。计数器显示 fetched 8,216 / committed 8,215、`delivery_retries` 仅 15，**未卡死、不丢数据**；影响的是索引新鲜度。空闲窗口应能使其排空，但需要观察。这条同时说明适配器的稳态吞吐余量很薄，V07 应实测其上限。

### 2026-09-30 两条滚动部署约束（复核发现，均已实际发生）

**1. 共享 `rawevent` 语义的二进制必须同批部署。** `payload_hash` 的算法在 `rawevent.New`（计算方，ingest）与 `rawevent.Validate`（校验方，raw-indexer、normalizer）之间共享，adapter 另有一处比对 receipt。只更新部分二进制时，新版生产者产出的信封会被未重启的旧版消费者判为 `payload_hash mismatch`，**整条原始流进 DLQ**。本轮已实际发生约 11 分钟（见「已知数据缺口」一节）。当前没有版本协商机制：信封里的 `schema_version` 不区分哈希方案，所以无法在运行期识别混版。缓解手段只有部署顺序纪律；**彻底修复需要给摘要加自描述前缀（如 `canon:v1:`）并在 `Validate` 对未知前缀显式报错**，属未做的设计项。

**2. 启用 `file_identity.fingerprint` 会改变位置形式，从而改变事件身份。** 位置由 `filebeat-v1:<device>:<inode>:<offset>` 变为 `filebeat-v2:<fingerprint>:<offset>`，`StableID` 随之变化。对已在接入的来源滚动启用时，24 小时积压被重读，且因身份不同而**作为全新事件重复入索引**，去重不生效——本轮实测 v2 位置文档 15,246 条与 v1 历史并存。要在不产生重复的前提下切换，须在来源暂停时进行，或接受一次性重复并记录。

### 2026-09-30 V07 单节点容量实测（首轮，稳态）

按 A03 约定范围在 248 上实测当前单节点负载，数据采集自源 Topic offset 增量、消费组 lag、进程 RSS 与 PostgreSQL 统计。

| 指标 | 实测 |
| --- | --- |
| 摄取速率 | conn 5.6 + dns 2.5 + 139 0.6 + 169 0.1 = **8.7 条/秒**（当前静默期；非峰值） |
| 端到端新鲜度 | zeek raw lag 6 条、tenant_a raw lag 0、标准 `network` lag 0 → 实时 |
| 积压恢复 | raw-indexer 消费约 4.3 条/秒且 lag 保持 0；此前清理污染区时实测约 **250 条/秒** |
| 内存 | 七个 TUBA 组件 RSS **合计 180 MB**（最大 normalizer 35.8 MB） |
| 磁盘 | 根盘 30,975/44,431 MB（70%）；Kafka 5,510 MB、ES 4,723 MB、PG 2,170 MB |
| PostgreSQL | 提交 5,888,316 / **回滚 30**（回滚率可忽略）；`ingest_receipts` 占库 2,158 MB |

**结论——最窄的一段是 source-adapter，不是索引侧：** 139 首轮 24 小时补采时实测达 **424 条/秒**，而适配器的稳态处理速率约 **3–4 条/秒**（逐条 HTTP 加一次同步 offset 提交）。稳态够用，但**任何来源突发都会让适配器持续落后**；raw-indexer 有约 250 条/秒余量，索引侧不是瓶颈。V02 的故障矩阵应把"适配器在突发下的滞后与追赶"作为重点，而不是索引吞吐。

**未测项**（本轮未做，V07 未勾选）：约定负载下的端到端 P95 延迟、峰值 EPS、内存与 PG 事务预算的上限压测、多来源同时突发。以上数据是当前静默期的稳态快照，不能外推为容量上限。

### 2026-09-30 异机备份首轮落地（B04 部分完成；V08 未通过）

备份目的地按用户决定选 21（可用空间 130 GB+）。架构：**ES 快照直接写进 21 上的仓库** `/opt/tuba-backup/esrepo-248`（NFS 导出后挂载在 248 的 `/var/lib/elasticsearch/backups`；`path.repo` 路径不变，故 ES 无需重启），PostgreSQL 转储、Keycloak realm 导出、发布包经 rsync 推到 `21:/opt/tuba-backup/248/<stamp>/`。`scripts/backup_tuba_to_offsite.sh` 每日 02:37 执行，另产出一份记录水位（库大小、receipt 行数、来源数、release 数）的 MANIFEST。免密通道为 248 上限定来源地址的密钥。

**已验证可用的部分**

- 完整跑通一次：转储 188 MB（源库 2,172 MB）、390 个归档条目、realm 61 KB、发布包、快照 `state=SUCCESS` 48/48 分片、MANIFEST 落盘，均已在 21 上复核。
- **根盘全程不变**：4.9 GB 快照写入期间 248 根盘稳定在 31 GB / 71%（旧架构同一操作会到 81%）。
- 仓库增量共享生效：第二个全量快照几乎不增加占用，21 上仓库稳定在 4.7 GB。
- **快照可恢复，且文档数与线上逐项精确相符**：恢复到临时索引名后 `_count` 比对 —— raw 794,004 / quarantine 54,767 / dns 73,849 / iam 1，全部 MATCH。

**本轮发现并已修复（严重）：备份每晚把平台打成只读**

旧架构把快照先写进 248 的本机仓库，这段临时空间（实测 4.9 GB）把根盘从 70% 推到 81%，**越过 ES 的 flood stage 水位（80%）**。ES 随即把**全部索引**置为 `read-only-allow-delete`，raw / quarantine / standard 三个索引器在 **10:27:08–10:38:11 连续约 11 分钟**被拒写：

```
raw evidence write failed after 5 attempts: raw document write returned 429:
cluster_block_exception: index [...] blocked by: [TOO_MANY_REQUESTS/12/disk usage
exceeded flood-stage watermark, index has read-only-allow-delete block
```

**没有数据丢失**：写失败不提交 offset，组件退出后由 supervisor 重启并从上次已提交位置重读；三个 DLQ 分段的最后写入时间为 02:01、02:01、04:47，**均早于事故**，事故期间 DLQ 零新增（末位偏移 tenant_a 509、zeek 9,565、source-adapter 10,470）。恢复后当日 raw 分区继续增长（41,968 条）。

修复即上文的架构改动：仓库移出本机根盘，快照不再经过本机磁盘。脚本前置两项检查——`ES_REPO_PATH` 必须是**独立挂载点**（否则拒绝运行，退出码 5），仓库文件系统占用不得超过 90%（退出码 4）；另有 `--check-only` 供窗口前预检。挂载点之下的目录已设为 root 只读，即使挂载缺失 ES 也只会报权限错，不会静默写满根盘。

**本轮发现并已修复（严重）：config 里的 70% 报警线成了本节点的分配硬上限**

`cluster.routing.allocation.disk.watermark.enable_for_single_data_node=true`（默认）使 **low 水位阻止一切新分片分配**，不只是副本。而 248 的常态占用恰好就是 70%，于是**索引创建与恢复时好时坏**，且 ES **不报任何错**：恢复在几十毫秒内以 `state [FAILURE]` 结束、**没有任何分片启动**，日志无分片级错误也无异常。本轮 4 分片的恢复测试失败 1–3 个、**每次失败的索引都不同**，正是这个原因；`_cluster/allocation/explain?include_yes_decisions=true` 在这种状态下还会误导——只给出 `restore_in_progress NO - shard has failed to be restored`，看起来像备份损坏。开 `org.elasticsearch.cluster.routing.allocation: TRACE` 才看得到真因：

```
DiskThresholdDecider: node [...] has 72.9% used disk
less than the required 13976562892 free bytes threshold (11.7gb free), preventing allocation
AllocationDeciders: Can not allocate [...]. [DiskThresholdDecider]: NO()
```

`13976562892` 恰为文件系统总量的 30%，即当时的 low 水位。**A03 的"根盘 70% warning"曾被直接用作 ES 的 low 水位，于是实际成了硬上限，不是告警档位。**

已按用户决定修复：`scripts/tuba_capacity_guard.py` 现在向 ES 写入 **low=75% / high=78% / flood=80%**，把告警档位与分配水位解耦——80% 仍是 A03 的停止写入线，新分片分配停在 75%（critical 线），70% 只作为上报档位由 `tuba_capacity_level` 指标承担。守卫已重启并生效。**验收：同一磁盘占用（71%）下重跑同样的 4 索引恢复演练，4/4 分片成功、文档数逐项 MATCH；改动前同一操作为 1–3 个失败。**

**未通过的部分（因此 B04 不勾选、V08 未通过）**

1. **未做全量恢复演练，且在本节点做不到。** 全量恢复要在线上数据之外再放一份完整证据库（4.9 GB），会把节点推到 80% 以上并再次触发分配失败——正是本轮踩到并查清的坑。本轮做的是**有代表性的子集演练**（含 1.4 GB 的大分片），文档数逐项相符；全量演练需要在空节点或独立实例上进行。
2. **未核对 Raw/标准/派生/案件水位，未实测 RPO/RTO。**
3. **PG 恢复演练无法执行**：TUBA 数据库身份没有 CREATEDB 权限，恢复需独立实例或具备建库权限的运维身份。
4. **节点剩下的分配余量很薄，仍未解决。** 水位解耦把可用区间从 0 个点扩到 4 个点（71% 常态 → 75% 才停分配），但 VG 已无空闲 extent，扩容只能加盘。占用一旦持续超过 75%，**新分区与索引创建会再次延迟**。加盘仍未做。
3. **PG 恢复演练无法执行**：TUBA 数据库身份没有 CREATEDB 权限，恢复需独立实例或具备建库权限的运维身份。
4. 未核对 Raw/标准/派生/案件水位，未实测 RPO/RTO。

**过程中造成并已修复的问题，记录以免重演**

- 本地快照清理与 cron 均以 `env -i` 最小环境验证过启用路径（cron 不加载 `/root/.bashrc`，缺 `LD_LIBRARY_PATH` 时 psql 会因找不到 libpq 失败）。

### 2026-09-30 监控缺口：消费组白名单漏掉整条 tenant_a 链路

排查"lag 数值冻结"时查出两件事，都不是 lag 本身的问题。

**一、当前量最大的接入路径没有 lag 监控。** kafka_exporter 的 `--group.filter` 是一份**手工白名单**（设计如此：退役代次会留下"有已提交 offset、无成员"的组，lag 冻结不动，若放开成 `tuba-.*`，`TubaKafkaConsumerInactiveWithBacklog` 会在它们身上常驻误报）。这份白名单共写在三处——`scripts/manage_tuba_monitoring.py` 的 `MONITORED_CONSUMER_GROUPS`、`rules/kafka.yml` 的两条告警、Grafana 面板查询——**三处都只列了 `zeeklive20260927*` 的组，一个 tenant_a 组都没有**。漏掉白名单不会报错，只会静默不监控：Windows Security 那条链路（09-29 接入、当前主要增长来源）没有任何 lag 告警，而安静的 Zeek 链路全程有。已补齐：受监控组由 11 个增至 19 个，其中 tenant_a 相关 8 个。

**二、白名单必须排除退役代次，否则立刻误报。** 第一版把三个无后缀的 `tuba-source-adapter-<hash>`（`c169402d…`、`d1ff4e04…`、`395791423…`）当成 tenant_a 的适配器加了进去，"有积压且无成员"的判定随即命中这三组（lag 14176 / 5575 / 303）。核查确认它们是**已撤销的 placeholder 来源**留下的孤儿组：`CONSUMER-ID`/`HOST` 均为 `-`（无成员）、committed offset 30 秒内一动不动，且它们消费的 `ctx_6000…/7000…/8000…` **不在 `source_instances` 里**；真正承载 Windows 数据的是带 `-zeeklive20260927r2` 后缀的 `3a5f5426333adfdd` 与 `91f5ede6aee00ba0`（lag=0、有成员），而这两个此前也不在白名单里。已改为按后缀模式匹配（`tuba-source-adapter-[0-9a-f]{16}-zeeklive20260927r2`），既覆盖新注册来源又排除孤儿。**验证：19 个组导出、三个孤儿组导出 0 条序列、"无成员且有积压"命中 0、三条 Kafka 告警均 inactive。**

**遗留**：白名单仍是多处重复（含 Grafana），**新增命名空间时漏改一处不会报错**，集中生成应在 D1.5 处理。孤儿消费组本身未删除（保留其 offset 作为证据），因此 `--all-groups` 排查时仍会看到冻结 lag，RUNBOOK 的 TubaKafkaLag 已写明如何区分。

### 2026-09-30 COL-07/V02 非破坏性子集（1/4）：重复投递与跨 offset 重发 —— 通过

**要验的不变量**：同一条记录被重复投递、或从不同 Kafka offset 再投一次时，不得产生第二条原始事件、不得丢已确认记录。这既是 COL-07 的"重复 offset/跨 offset 重发"，也是 V02 的"重复提交"，并且是采集端强杀后重读场景的安全前提。

**方法**：直接按 adapter 面向 ingest 的契约投递——`POST /api/v1/internal/ingest/beat-events`，头带 `X-Source-Adapter-Token` 与 `X-Source-Topic/Partition/Offset`，body 为源 topic 里该记录的**原始字节**。选真实记录（不从构造输入），跑在 248 本机、不经过 adapter，因此对线上无副作用。

**为什么不用 ES 文档数判定**：重复投递在 receipt 层就被吸收，第二条永远不会写进 Kafka，所以 ES 文档数看不出差别。**唯一可观测的位置是 receipt 本身与 raw topic 里的出现次数。**

| 用例 | 记录 | 三次投递（同位置 / 完全相同 / 换 offset+777） | raw topic 中该 `raw_event_id` 出现次数 |
| --- | --- | --- | --- |
| Windows（tenant_a） | `ctx_03329669…` offset 5511 | 均 HTTP 202，`receipt_id` **完全相同** `raw:af2c9509…` | 扫描 3,000 条，**1 次** |
| Zeek（tls） | `ctx_5fadf1a6…` offset 128708 | 均 HTTP 202，`receipt_id` **完全相同** `raw:c5e0b697…` | 扫描 3,000 条，**1 次** |

另验冲突分支：**同位置、改一个字段**再投 → **HTTP 409** `source position was already used for a different payload`，不产生新事件。这正是 2026-09-29/30 那批 DLQ 的来源类别（当时是采集端重读时来源字段渲染不稳定所致），现在确认它在 ingest 侧被正确拒绝而不是静默接受。

**结论与机制**：去重键是 `RawEventID`（由**稳定位置**推出，Windows 是 computer/channel/recordID/timestamp，Zeek 是文件指纹+偏移）加 `PayloadHash`，**不含投递位置**。所以采集端强杀后重读、或消费组回退 offset 导致同一记录从新 offset 再来时，会收敛到同一条原始事件——这正是"强杀重读"能够安全的前提，也在契约层面解释了为什么位置稳定性（`internal/rawevent/beat_position.go` 的指纹方案与 `canonical.go` 的规范化哈希）是必需的而不是优化。

**过程中修正的一处判据错误**：本测试第一版把"raw topic 末位不得增长"当作判据，**该判据无效**——raw topic 同时在接收实时业务流量（测试期间 tenant_a +4、Zeek +121），无法区分是我的投递还是生产写入。已改为统计 `raw_event_id` 在 raw topic 中的出现次数，对实时流量免疫。

**本项未覆盖（仍属 COL-07/V02 未完成部分）**：采集端真实强杀与 registry 丢失窗口（下一项已补）；ingest 不可用时 adapter 不提交 offset；DLQ 不可用时不得提交输入。

### 2026-09-30 COL-07/V02 非破坏性子集（2/4）：采集端强杀后重读与补齐 —— 通过

**要验的不变量**：采集进程被强杀后，不得丢已产生的记录，也不得把重读的记录变成重复。

**方法**：对 21 上 r2 的 `ssl` Filebeat（数据面 4 个独立实例之一，独立 config/registry/queue/ACL）发 `kill -9`，保持停机后重启，用**停机期间源日志新增的记录**做端到端对账。

| 观测量 | 值 |
| --- | --- |
| 强杀前 1 分钟自然速率 | ssl 源 topic 129,964 → 129,976（12 条/分） |
| 强杀时刻 topic 末位 | 129,986（停机期间**不再推进**，采集端是唯一生产者） |
| 停机期间源日志 | 654 → 693 行，即 **39 条待补** |
| 恢复后 topic 末位 | 130,046，**+60 条被补发**（≥ 39 条待补） |
| 停机前最后一条记录（uid `CoWaiP2JZLGhn2EWu3`） | **在 topic 中查到** → 停机窗口被完整覆盖，无空洞 |
| adapter `events_rejected_total` / `dlq_written_total` | 6,327 / 6,327，**零新增** → 重读被 receipt 去重吸收，无冲突 |

**registry 行为**：`data/ssl/registry/filebeat/log.json` 在强杀前几秒仍在写入（mtime 实时更新），且配置为 `filebeat.registry.flush: 1s`，因此强杀最多丢失 ≤1 秒的书签；重启后从该书签继续，落在窗口内的记录会被重发并被去重。

**处置中暴露的两个运维缺口（非本次测试目标，但必须记录）**

1. **r2 管理器没有"单个数据集"的启停。** `start` 在任一实例运行时直接拒绝并启动全部四个，`stop` 也是全部；参数化调用（`start ssl`）不被接受。这意味着**线上单个数据集故障时，无法只重启它**——只能停掉并重启全部四个。本轮因为按参数调用失败，实际造成**四个 Zeek 数据集约 4.5 分钟的非预期停机**：没有丢数据（registry 保留、重启后补齐已逐项核对），但这暴露了处置能力的缺口，属 COL-09 范围。
2. **`run/processes.json` 会留下已死进程的条目。** `status` 能正确报 `stopped/stale`，但 `start` 的存在性检查会把它算作"运行中"而拒绝启动。两者判断口径不一致。

**局限**：本次强杀的重读窗口只有 ≤1 秒（配置决定），按当时 12 条/分的速率命中概率约 20%，因此**"重读不产生重复"这一半不是靠这次强杀证明的**，而是在上一项用 ingest 层的确定性重复投递证明的；本次证明的是"不丢"与"无冲突"。

### 2026-09-30 COL-07/V02 非破坏性子集（3/4）：日志轮转不丢记录 —— 通过，并查清一处每小时发生的整份重读

**要验的不变量**：轮转（live spool → 小时归档）不得丢记录，也不得产生重复文档。

**方法**：`sync-archives` 每小时把 Zeek 的压缩归档解压安装为 `archive/<dataset>/<dataset>.HH:00:00-HH:00:00.log`。借刚发生的 14:00 轮转做回溯核对：取 `ssl.13:00:00-14:00:00.log` 的**每一行**，与 ssl 源 topic 中已发布消息的 `event.original` 逐行比对（按行哈希，不用 uid——uid 在 Zeek 里不是唯一标识，一条 TLS 连接可写多条 ssl 记录）。

| 观测量 | 值 |
| --- | --- |
| 归档文件行数（去重后仍同值，文件内无重复行） | 770 |
| 在 topic 中找到的行数 | **770 / 770，丢失 0** |
| 边界连续性 | 轮转前文件止于 ts 1790747991.78，新 spool 起于 1790748001.71，**无重叠、无空洞** |

**同时查清一处每小时系统性发生的整份重读（比测试本身更重要）**：这 770 行**每一行都被发布了两次**——一次 `log.file.path` 为 `/opt/zeek/spool/zeek/ssl.log`，一次为 `/opt/tuba/collector-live/filebeat-r2/archive/ssl/ssl.13:00:00-14:00:00.log`。即 `sync-archives` 把整点归档解压进 archive 目录后，**Filebeat 把整整一小时的文件完整重发了一遍**——每小时、每个数据集都如此。

**正确性未受影响**：抽查 272 条重读记录，**每一行只对应一个 `source_position` 和一个 `raw_event_id`**。指纹身份是内容前缀哈希，spool 与 archive 两份内容逐行相同故指纹相同，稳定位置一致，第二份被 receipt 去重吸收，**ES 无重复文档**。

**这条发现解释了此前的 DLQ 缺口**：重读不是"强杀才发生"的稀有事件，而是**每小时系统性发生**。所以当时任何 payload 不稳定（Windows 任务名渲染、`log.file.path`/inode 差异）都会在这一次整份重读中**批量**触发 `payload_hash mismatch` 冲突——这正是起初那批 DLQ 有数千条而非几条的原因，也从机制上确认了当时"把 collector 传输元数据排除出规范化哈希"的修复方向是对的。

**代价（已量化，未处理）**：归档数据在 source topic 里翻倍。按 ssl 每小时约 770 条、四个数据集计，每小时约 3,200 条额外消息；下游去重能吸收，但 adapter、ingest、receipt 都要多处理一遍。不是正确性问题，在当前量级下也不是容量问题，属**可优化的浪费**。要消除需确认 Filebeat 的 registry 是否按 path+fingerprint 双键记账——若 archive 副本出现时能命中同一 fingerprint 而不重新收割，即可省掉这一次整份重发。

### 2026-09-30 COL-07/V02 非破坏性子集（4/4）：归档 spool 回放与确认水位 —— 通过，并修掉两个缺陷

**要验的不变量**：归档 stage **只在 Filebeat 证明读到 EOF 之后**才回收；不得仅凭"文件超过 N 小时"删除未确认输入。

**核对方法**：把 ssl 的 stage 文件年龄与合并后 registry 的游标逐条对列，而不是只看汇总计数。

**闸门本身是正确的**：目录里的 6 个 `.log`（08:00–14:00）年龄 0.4–5.4 小时、**未到 6 小时阈值**，而其注册表游标**全部 `offset == size`**（已完整读完）。即"未过期不动、过期且确认才回收"，行为符合设计。既有 6 个单元测试也逐条覆盖该不变量（仅有年龄不得删除、游标未到 EOF 必须保留、未被跟踪必须保留、registry 不可读时 fail closed、解压中途磁盘满保留源文件），全部通过。

**核对中发现并修复的两个真缺陷**

1. **http 的 registry 从未写过快照，导致它的归档 stage 永远无法被确认、永不回收。** conn/dns/ssl 的 registry 目录都有 `<txid>.json` + `active.dat`，**http 只有 `log.json`**。对比退出日志：ssl 有 `Loading data file ... succeeded`，http 没有这一行——Filebeat 自己就是靠事务日志加载的。而 `_read_stable_registry` 硬要求 `active.dat`，对 http 抛 `RuntimeError`，于是 `archive-status` 里 http 恒为 `acknowledged_files: 0`，18–36 个 stage 长期滞留（14.5 MB，按约 10 MB/天增长，**无界**）。**修法有原则依据**：`active.dat` **不存在**时按"空快照 + 只读事务日志"处理（事务日志记录的是完整状态，Filebeat 自身就这么加载）；`active.dat` **存在但读不了**仍 fail closed——那才是真正的不一致信号。修复后 http registry 由"不可读"变为可读 25 条。
2. **`.source.json` 标记永不被回收，并污染状态输出。** `archive_status` 统计目录下**所有**文件，而标记永远不会"被确认"（注册表跟踪的是 `.log`），于是每个数据集恒报 `acknowledged_files: 0` 与一个永不下降的 pending——运维会把这条读成"闸门坏了"，而它其实只是把标记算进去了。stage 回收时标记也被留下成为孤儿。修复：状态只统计 `.log` stage；回收 stage 时一并删除其标记；标记的 stage 已不存在时按残留清除。

**部署后现场验证**：

| 指标 | 修复前 | 修复后 |
| --- | --- | --- |
| 孤儿标记 | 96 | **24**（清除 72，日志逐条 `removed orphaned archive marker`） |
| stage `.log` | 42 | **24** |
| 私有 spool 体积 | 49 MB | **34 MB** |
| `archive-status` | http 恒 0 确认、pending 不降 | 四路均 `expired=0 / pending=0` |
| http registry | 不可读 | **可读 25 条** |

修复只重启了 `archive-sync`，四个采集器的 pid 全部不变（正好用上新部署的单数据集启停）。

**一处运维要点**：`archive-sync` 是长驻进程，**替换管理器文件不会影响正在运行的它**——清理逻辑的变更必须重启 `archive-sync` 才生效。这一条已写入 RUNBOOK。

### 2026-09-30 来源生命周期三态上线（248），并纠正迁移状态不受管的问题

**上线内容**：迁移 `00014` + 新 `tuba-api` / `tuba-ingest` / `tuba-source-adapter`（ingest 与 adapter 按滚动部署约束同批重启）。

**现场验收（真实环境，非 mock）**：

| 场景 | 结果 |
| --- | --- |
| 被撤销来源的 topic 投递 | **HTTP 403** `source has been revoked` |
| 临时改为 `paused` 的同一来源 | **HTTP 503** `source is paused` |
| 不存在的 context | **HTTP 503** `source topic is not currently bound to an active source` |
| 活跃来源的 topic | **HTTP 202 accepted**（无回归） |

回填结果：6 条 `active` / 4 条 `revoked`，被标为 revoked 的正是那四个退役的 Zeek r1 来源（conn/dns/http/ssl）。一致性约束的**拒绝能力也现场验证过**：写 `enabled=false` 而 `state='active'` 直接报 `violates check constraint source_instances_enabled_matches_state`。上线后 20 秒采样：adapter `fetched` +60、raw 文档 +273、7 个组件存活、ES green、`rejected`/`dlq` 计数归零且无增长。

**过程中发现并纠正的一件事：248 的迁移状态此前不受管。**

- **没有任何迁移台账**（`public.tuba_schema_migrations` 不存在），主机上的 `migrations/` 目录**只到 00006**，而线上 schema 里 00007–00013 的表全都在——说明那七个迁移是从别处手工执行的，主机既没有台账也没有脚本。
- 这是个陷阱：直接跑仓库的迁移工具会从 00001 重放，撞上已存在的对象。
- 处理方式不是继续手工 psql，而是把它做成受管状态：**逐条核实**每个迁移创建的对象是否真的存在（00001–00006 在主机文件上核、00007–00013 用仓库文件核，合计 33 张表逐一确认）→ 用**主机上的实际字节**算出 00001–00013 的校验和补进台账 → 把脚本放到正确层级（`scripts/` 下，因为 `root="$(dirname $0)/.."`）→ 用工具应用 00014，输出逐条 `Skipping already applied` + `Migration state verified: 00014`。
- 另发现两处与设计不符、已记录待决：**运行时角色 `tuba` 是 schema owner，可以 DDL**（README 要求"应用账号只有 DML、迁移用独立 DDL 身份"，248 上从未实现）；`/opt/tuba/bin/` 堆着陈旧与重复制品（9/25 的 `tuba-ingest` 与在用那份 md5 不同、多个 `tuba-api.bak-*`），且 `run/` 里 ingest/indexer/analysis-sink 的 pidfile 是死的——**此时跑 `start.sh` 会把旧二进制拉起来**。

**同时实测确认了 O01 的两条不成立**（此前只能推断）：所有 TUBA 组件**以 root 运行**；启动方式是 `start.sh` 的 `nohup` 加 Python 监督脚本，**没有任何 systemd 单元、也没有 `tuba-launcher` 在跑**。

## 后续多节点（P2；本轮不要求实施）

- [ ] X01 Kafka 多 broker/控制器、Topic 副本/ISR、分区迁移与故障演练。
- [ ] X02 ES 多节点、副本、分片预算及冷热分层；PG 主备和切换入口。
- [ ] X03 ingest/API 负载均衡与分布式配额；身份服务及网关冗余。
- [ ] X04 entity/analysis 状态再分片、多 worker 租约/fencing 和有序重分配。
- [ ] X05 依据实际规模决定是否迁移 Kubernetes/Vault/专用状态存储，保持业务合同不变。

## 建议交付批次

| 批次 | 任务范围 | 可见交付 |
| --- | --- | --- |
| D1 数据底座 | A、C、O01/O02/O04、I、N、COL-01–05、COL-07（非破坏性子集） | 单节点可靠原始接入＋DIP/UIM＋八领域检索 |
| D1.5 来源与运行治理 | O03、COL-06、COL-08–15 | 过滤策略、Management Agent、管理 UI、更多来源 |
| D2 分析主体 | T、E、F | 可恢复任务、实体、多角色、特征、基线和检测 |
| D3 调查产品 | R、Q、W | 风险、案件、反馈、管理和受控查询 |
| D4 运行交付 | B、全部 V、COL-07 破坏性子集、文档收口 | 可安装、可恢复、可运营的完整单节点版本 |

T 可在 D1 后半段开始；Q/W 在对应 API 合同稳定后逐步交付。容量与验收用例设计从 A/C 阶段开始，执行随实现推进；不把全部可靠性问题留到 D4 才发现。

### D1 的完成定义（2026-09-30 用户确认）

**D1 只回答一件事：数据能不能可靠地进来、并检索到。** 据此划定边界：

| 阶段 | 计入 D1 的范围 | 状态 |
| --- | --- | --- |
| 阶段 0 | `A01–A05`、`O01`、`O02`、`O04` | A 全部完成；O 的代码已落地，缺验收证据 |
| 阶段 1 | `C01–C09` | **全部完成** |
| 阶段 2 | `O05` | **已完成** |
| 阶段 3 | `I01–I06`、`COL-01`–`COL-05`、`COL-07` 非破坏性子集 | 两条来源链路已在 248 跑通，多数缺验收证据 |
| 阶段 4 | `N01–N08` | Normalizer/DIP/UIM/Quarantine 在线上运行，缺验收证据 |

**明确移出 D1 → D1.5**：`O03`（反代/TLS/最小权限）、`COL-06`（过滤策略版本与影子计数）、`COL-08`/`COL-09`（Management Agent 与受管监督器）、`COL-10`（管理 UI）、`COL-11`–`COL-15`（Syslog/JumpServer/Keycloak 连接器、连接器框架、旧路径迁移）。

理由：这十项回答的是**"来源能不能被远程治理、能不能再接入更多来源"**，而不是"数据能不能进来"。它们既不阻塞 D1 交付，也不阻塞 D2 开工；留在 D1 里只会让 D1 永远画不上勾。作为代价要认清：当前 10 个来源全部由运维脚本手工管理（Windows 侧是脚本 + WinRM），这是 D1.5 要解决的真实缺口，但它不改变 D1 的判定。

**D1 画勾前必须留下的证据**（每项勾选都附证据，不接受"应该没问题"）：

1. 两条真实来源链路（Zeek 四 dataset、Windows Security）在 248 上有**逐段计数与确认水位**记录；
2. `COL-07`/`V02` 的**非破坏性子集四项全部通过**（重复投递与跨 offset 重发、采集端强杀、日志轮转、归档回放与确认水位）——前三项已于 2026-09-30 完成；
3. `O04` 的目标主机重启恢复验收；
4. `N07`/`I06` 的"暂时重试 vs 永久 DLQ"与容量/过期保护状态的**可查询**验收。

**不计入 D1 画勾前置**：`COL-07` 的破坏性子集（磁盘满、Kafka/PG 故障、Topic 重建、目标机重启、积压换凭据）——它依赖加盘扩容，归 D4；`V01`–`V10` 的其余项同样归 D4，但其中的非破坏部分可在 D1 期间就做（已在做）。

## 2026-09-26 Zeek 标准事件纵向验收记录

- 21 上 Collector 投递的 16 条匿名化 Zeek conn 记录，已从隔离 Raw topic 经 Normalizer/DIP/UIM 输出到 `tuba.collector.validation.events.network.v1`，再由标准 Indexer 写入 `logs-ueba.network-tenant_a`。
- ES 验证：16 hits、16 个唯一 `event.id`、16 条 route.domain=`network` 且 quality=`qualified`；事件按其 Zeek `ts` 分布到 UTC 日期分区。Raw ES 仍有对应 16 条证据。
- 验收中依次发现并修复 Zeek DNS/HTTP/TLS `event.dataset` 未随 route 更新、UIM mapping JSON 不完整；分别增加 dataset 映射单测和严格 mapping JSON 回归测试。第一次失败投递保留在隔离 DLQ `tuba.collector.validation.dlq.v1`，成功重放后的标准事件和 Raw 证据保留在 ES。
- 新增受限 `GET /api/v1/events`，限定八个领域、租户和时间窗，使用 alias 与游标分页；返回字段排除 `vendor.payload`。API 查询逻辑已有隔离 ES mock 测试，但真实 OIDC 登录后通过该 API 查询尚未验收。2026-09-26 已恢复 248 上新版 API，并通过 8443 HTTPS 入口验证 health 200 与无令牌 events 401；尚缺有效 OIDC 用户令牌以完成授权查询。
- 代码测试：`go test ./...` 全部通过。剩余 N01/N03/N04/N06 和 Q01 属于部分实现，不能据此勾选阶段 4 或产品端到端 V09。
- 2026-09-26 在 248 部署本工作区构建的 `tuba-api`；旧 API 因 `DATABASE_URL` 密码含 URL 保留字符且未编码而不能解析。先备份旧二进制与配置，再仅对 DSN userinfo 密码做百分号编码。现 API 绑定 `127.0.0.1:8788`，8443 HTTPS server 将 `/api/v1/` 和 `/health/*` 代理到 API；从开发机验证 health 为 JSON 200、无令牌 events 为 401，直连 `:8788` 被拒绝。后续开发阶段使用 HTTP 即可，证书信任/主机名不纳入当前开发验收；生产 O03 仍需配置有效 HTTPS。
- 8081 的 502 与 TUBA API 无关：248 的该端口由 ADMS webserver 提供，配置转发到 `localhost:18081` 的 `adms_flow_cluster`；该 upstream 当前不可连接。未改动 8081 路由；新增 TUBA 路由仅在 8443 TLS server。
- 2026-09-27 已在 247 重建开发用 `tuba` realm，HTTP OIDC discovery 返回 200，issuer 与 248 配置一致；`tuba-web`/`tuba-api` 客户端、PKCE S256、API audience、TUBA 角色与 claims mapper 已验证。当前 realm 不包含演示用户，AD 用户联邦尚未配置，因此真实普通成员登录及授权查询仍待完成。
- 开发阶段通过 HTTP 验收；8443 TLS 主机名/证书不构成当前阻塞，也未发送 bearer token。普通用户 API 验收等待 AD 联邦与租户成员映射完成。
- 后续只读检查确认 248 上 `tuba-api` 监听 `127.0.0.1:8788`，`webserver` 监听 `0.0.0.0:8443`，ES 监听 9200。8443 当前证书为自签 `CN=riversecurity.com` 且 SAN 不匹配 `10.6.68.248`；仅作为生产 O03 的待办诊断记录，开发阶段使用 HTTP，不构成当前验收阻塞。
- 标准 indexer 已实现 UIM bulk 写入并读取配置中的批量大小/等待时间；`go test ./...`、`go vet ./...` 通过。2026-09-26 在 248 用独立组 `tuba-standard-indexer-network-tenant_a-bulk-validation-20260926` 重读 `tuba.collector.validation.events.network.v1`：CURRENT-OFFSET=16、LOG-END-OFFSET=16、LAG=0；`logs-ueba.network-tenant_a` 仍为 16 条。隔离 DLQ 中该输入 topic 的 16 条记录均为既有 mapping 故障 `ES_INDEX_CREATE_FAILED`，没有 `EVENT_ID_CONFLICT`。此次未重置原消费组 offset 或写入新事件。
- 回归新增 ES create-409 等价内容核验与冲突隔离测试：等价 `_source` 视为成功，内容不同时返回 `EVENT_ID_CONFLICT`；worker 将永久冲突写入 DLQ 并在确认后提交。全量 Go 测试通过。
- 2026-09-27 完成 N06 隔离端到端验收：Kafka `10.6.68.71:9092` 新建 `tuba.n06_20260926_001.*` 隔离 topic；本地标准 Indexer 连接 ES `10.6.68.248:9200`，使用 namespace `n06_20260926_001` 和独立消费组。向八个领域各写入 1 条合约有效 UIM 事件，并向 network 写入 2 条同 `event.id`、不同内容的事件。八个 ES alias 均建立成功，authentication/session/iam/directory/dns/web/tls 各 1 条，network 2 条；物理索引日期为 `2026.09.26`。冲突事件进入隔离 DLQ，原因码为 `EVENT_ID_CONFLICT`；network 输入 offset 3/3，其余 topic offset 均为 1/1，全部消费组 lag=0。只使用全新隔离 topic/group/namespace，没有读取或重置既有消费组 offset。验证索引、topic 和 DLQ 记录保留以供复核。
- 2026-09-27 在 247 重建 TUBA 开发 realm：从本地 realm 定义派生独立文件 `deploy/keycloak/tuba-realm-247-dev.json`，移除 3 个示例用户、关闭 `tuba-web` direct access grants、强制 PKCE S256；保留 `tuba-api` audience 与 `tuba_roles`/`organization_id`/`namespace` claims mapper，并补齐 `platform_admin`、`publisher` 角色。通过 Keycloak Admin API 创建 realm（HTTP 201）；discovery issuer 为 `http://10.6.68.247:8180/realms/tuba`，JWKS 返回 2 个签名密钥，客户端/角色/claims 均经 Admin API 核验。realm 当前没有演示用户，也尚无 LDAP provider。只读探测确认 AD 域为 `test.local`（`DC=test,DC=local`），现有目录中未发现 `svc*` 账号或 `TUBA*` 组；未将域管理员凭据写入 Keycloak。后续需创建/提供专用只读 LDAP bind account，并确定 AD 用户到 TUBA tenant/namespace 的成员映射后才能启用 AD 登录验收。
- 2026-09-27 登录验收续测：247 Keycloak realm 和 248 `tuba-api` 就绪，API `/health/ready` 经本机 SSH HTTP 隧道返回 200。为 `tenant_a` 创建了短时验收用户并授予 analyst membership，但浏览器控制策略拒绝打开 `http://127.0.0.1:5173/`（URL policy），并明确禁止改用其他浏览器、CDP 或间接方式绕过；因此未完成浏览器 Authorization Code + PKCE、`/api/v1/me` 或事件查询的登录后验证。已立即删除 Keycloak 验收用户，并在 248 数据库事务中删除对应 membership/identity，复查 identity 行数为 0；关闭本机 Vite 和 SSH 隧道。登录验收仍待在获准的浏览器会话中完成，不能标记通过。
- 2026-09-27 本机登录页恢复：应用户反馈重新启动 Vite（`127.0.0.1:5173`）并建立 248 API SSH HTTP 隧道。验证登录页返回 200，API readiness 返回 200，未登录的 `/api/v1/me` 返回预期 401。前端与隧道目前保持运行，供浏览器刷新；登录后的验收仍未完成。
- 2026-09-27 为继续手动登录验收，已在 247 临时重建 `tuba-acceptance-20260927` 开发用户，并在 248 PostgreSQL `tenant_a` 授予 analyst membership。该身份仅用于本轮验收，尚未完成登录验证，验收后需删除用户和成员记录。
- 2026-09-27 针对 Keycloak `VERIFY_PROFILE` 页面修正验收账号：补齐 first/last name，并把 `organization_id`、`namespace` 注册为仅管理员可编辑的用户属性后写入 `tenant_a`；账号无待执行 required action。这样用户认证只需用户名/密码，租户 claims 仍由管理员配置。当前浏览器已有的认证流程可能仍保留旧的 profile action，需返回应用重新发起登录；尚未取得登录后 token/API 验收结果。
- 2026-09-27 登录回跳排查：用户确认浏览器先到 Keycloak 再回 TUBA 首页。前端现有逻辑在 `/api/v1/me` 收到 401 时静默清除 token 并回到登录首页；已修改 `web/src/auth.tsx` 在此处显示明确的 401 提示。Vite 已热更新；账号 profile 的 `organization_id=tenant_a`、`namespace=tenant_a` 与 analyst membership 已配置。需再次登录以判断是否仍为 API access token 验证失败；尚未确认 `/api/v1/me` 成功。
- 2026-09-27 按要求将本机开发登录改为用户名/密码表单，移除企业登录按钮和 OIDC 回调页；表单将密码直接提交给 247 开发 realm 的 Keycloak token endpoint，提交后清空密码输入框。仅 247 的 `tuba-web` 开启 Direct Access Grants，realm 定义同步到 `deploy/keycloak/tuba-realm-247-dev.json`。已在浏览器完成测试账号登录并到达 `/overview`，页面显示 `tenant_a` 和测试 subject，证明 `/api/v1/me` 已接受令牌且 analyst membership 生效。总览数据仍显示 `data store unavailable`，属于登录后的数据读取问题，尚未排查；临时账号保留以便继续验收，验收结束后清理。
- 2026-09-27 登录验收收尾：已定位 `/overview` 报错原因是 tenant_a 尚无 `ueba-anomalies-tenant_a` 索引；ES 集群本身正常。API 对缺失异常索引按空结果处理，并对异常列表采用相同规则；在 248 更新 `tuba-api` 后 readiness 返回 200，浏览器异常列表显示 0 条、总览显示异常/案件均为 0，登录态与租户授权正常。已从 247 `tuba` realm 删除临时用户 `tuba-acceptance-20260927`，并确认 248 PostgreSQL 中该 subject 的 identity 与 tenant_a membership 均为 0。247 开发 realm 的 Direct Access Grants 保持开启，以支持当前开发阶段的用户名/密码登录。

### 2026-09-27 Zeek Collector 合成闭环续验（更新 A04/COL-03/COL-04/N03 状态）

以下仅为当时的合成验证快照；后续真实 Zeek 接入已完成，现状以本清单 A04/COL-04、上方后续顺序及 [环境报告](ENVIRONMENT-248-REPORT.md) 为准。

- 248 PostgreSQL 迁移：迁移前备份 `/opt/tuba/backups/pre-collector-migrations-20260927.dump`，SHA-256 `d7be19f0ee066d40a3ee9e590abec2745cb69b6e0024bc8c0fe6e2113a411673`；00007–00011 回滚演练后按初始化策略前滚，00001–00006 登记为基线。
- 隔离上下文：创建验证 organization/release 和四个正确 dataset=`zeek.conn/dns/http/ssl` 的 source contexts。首轮缺少 `zeek.` 前缀而被 DIP 正确隔离；停用旧 context、保留记录，再创建新 context，验证来源语义不可被原地改写。
- 运行结果：21 独立 Filebeat → 71 合成专用 Topic → 248 source-adapter/ingest receipt/Raw → DIP/UIM → network ES 成功；conn 标准事件保留原文且端点与 300 bytes 正确。Raw alias 共 2 条（含失败轮的原始证据），network alias 1 条 qualified 事件。首轮错误记录通过 quarantine Topic 被专用 indexer 写入 `logs-ueba.quarantine-zeek_validation_20260927_001`，ES count=1。
- 21 临时验证 Filebeat 已停止，既有 systemd Filebeat 仍为 active；未修改其服务、配置或 registry。248 验证数据面经启动器停止并确认 `stopped`。启动器已加入限定到所记录验证进程组的 SIGKILL 超时升级。
- A04/COL-03/COL-04/N03 继续保持未完成：以上是合成数据的路径验收，不代表共享明文 broker 的生产隔离。71 无 ACL、248 broker 对外通告 localhost；需先提供具备可用远端 advertised listener 和 ACL 的接入 Kafka，再验收真实 Zeek 数据、四类 dataset、轮转/归档恢复及 Beat 管理。\n

- 2026-09-29 T04 现场部署：248 的数据库实际使用旧版 `public.schema_migrations(filename, applied_at)`，迁移最新到 00011；已确认 00012 前置不一致 source_context 为 0。基于用户“你帮我执行”的明确授权、已有完整 PostgreSQL 备份 `/opt/tuba/backups/pre-release-publishing-20260929.dump`（SHA-256 `e43e88d7a55911c2af16e0a61ced7bcfbd395db0b31f5cdfe309dcecf0b3c935`），以现有 `tuba` 数据库身份、持有 `74190024001` advisory lock，分别事务执行并登记 `00012_source_context_scope.sql`、`00013_release_publishing.sql`；读回两条 ledger 记录、发布者/idempotency 表及两个触发器均存在。未修改数据库角色/授权。248 `/opt/tuba/bin/tuba-api` 已备份到 `tuba-api.pre-release-publishing-20260929` 后原位切换；配置备份为 `tuba.env.pre-release-publishing-20260929`，新增 `TUBA_RELEASE_ROOT=/opt/tuba/releases`。API readiness 200；首位 publisher 已用一次性工具授予当前 operator subject `26a64c67-b05a-4d52-b08f-201a8657ec03`，审批引用 `user-authorization-20260929` 已进入 bootstrap 审计。bundle 7 个 asset 在 248 的 hash 校验通过。发布 API 的实际浏览器操作还未执行：原浏览器 access token 已被 API 判为无效（401），当前需重新登录后继续；未创建 Windows release row，也未推进其状态。故 T04 保持未完成。
