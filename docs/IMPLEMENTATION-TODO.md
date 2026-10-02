# TUBA 完整架构实施 TODO

日期：2026-09-25｜采集方案修订：2026-09-27 v2｜基线：[TARGET-ARCHITECTURE.md](TARGET-ARCHITECTURE.md)

本清单覆盖完整单节点产品目标。未勾选项仍未按本基线验收；旧 M0–M5 完成记录不能直接关闭新任务。仓库和 248 开发环境包含多阶段代码切片并完成过部分隔离/登录验收，但不代表所有切片都已部署或整阶段通过；只有达到任务完成条件的条目才勾选。

状态覆盖说明：A03 已于 2026-09-28 定版并关闭。下方较早验收记录中“待 A03”“A03 未定”或旧 7d/14d Kafka 目标仅保留当时背景，不再表示当前状态；当前权威边界是 Kafka 24h、ES 7 日、Zeek 四类来源以及根盘 70/75/80% 水位。

设计状态说明（2026-09-29）：完整单节点范围内此前开放的产品设计选择已在 [产品详细设计基线](DESIGN-BASELINE.md) 定版，包括 Windows Security 范围、UIM 质量边界、来源生命周期、过滤、任务一致性、实体归因、特征/基线/风险、SPL、导出、管理界面、生产传输、备份恢复、回放切换与验收证据。下列未勾选项继续表示代码、迁移、部署或实际验收未完成；不得因设计定版而勾选。备份接收端、证书 CA、告警 receiver 等是环境配置输入，缺失时必须 fail closed，不再作为设计开放问题。

## 当前代码切片（未等同于阶段验收）

- Raw：`internal/rawevent`、`internal/ingest`、`internal/rawindexer` 和 `internal/sink/raw.go` 已有可信单事件 envelope、PostgreSQL receipt、Kafka 确认后 202、按 UTC 接收日写 Raw ES。
- DIP/UIM：`internal/uim` 与 `internal/normalizer` 已有 Microsoft Windows Security 的一组认证/IAM/目录事件和 Zeek conn/dns/http/ssl 首批映射；合同草案位于 `contracts/uim/domain-contract.v1.yaml`。
- 索引：标准八领域和隔离索引 worker 已创建，ES 写入采用固定 UTC 日物理索引及逻辑 alias。
- 控制面：`internal/controlworker` 与 `cmd/tuba-control-worker` 已有 PostgreSQL outbox 的有界领取、聚合键顺序投递（同键严格顺序、不同键有界并发）、租约 fencing、Kafka 至少一次投递、有界重试（耗尽行 fail-closed 阻塞键并可重排队）与卡死/超时告警（计数器+积压 gauge，见 T03 记录）；任务 worker 框架支持注册 handler，并做 queued 取消及过期租约回收；任务状态机/合法迁移/有界退避已形式化为 `state.go` 唯一权威（见 T01 记录）；retention 清理器（inbox/outbox/任务 state/任务临时目录，fail-closed 保护）已作为第三个周期任务接入（见 T05 记录）。`migrations/00008_worker_leases.sql` 补充 outbox/job 字段。当前入口没有注册业务 handler，任务执行/API、审计和运行验证仍待完成。
- 来源管理：`internal/control/sources.go` 与 `internal/ingest/source_registry.go` 提供租户内来源登记/列表/撤销、API Key 摘要存储和来源级限速；COL-01 切片已加入不可变 source context、24 小时旧 key 重叠和不变更 epoch 的轮换。receipt 持久化 envelope 与 Kafka ACK 状态。
- 旧自研 Collector 代码切片（冻结新增通用采集需求）：`cmd/tuba-collector`、`internal/collector` 提供 SQLite WAL 队列、事务性入队游标、单条 Sender、Zeek JSONL 完整行读取和 equality 过滤骨架；未通过旧自研方案的完整验收，自研 Windows adapter 计划已取消，改由新 COL-05 交付 Winlogbeat 接入包。
- Collector 控制面服务端切片：迁移 `00011_collector_management.sql`、`internal/control/collectors.go`、`internal/api/collectors.go` 与 OpenAPI 已提供一次性注册 token、Agent 凭据摘要、列表/禁用、心跳、版本化配置及 ETag 读取。ZIP 客户端尚未调用这些 API，不能视为远程管理闭环。
- 限制：Zeek 独立 Filebeat 与 248 隔离链路已完成真实数据部署和首轮闭环；过滤影子计数已于 2026-10-02 在 source-adapter 落地并现场验证（见 COL-06）；仍未完成永久拒绝隔离以外的准入执行（enforce）、新 Beat 来源位置/缺口适配、来源上下文发布/退休 API、端到端故障验收及通用远程部署管理。不能据此宣称 Collector 阶段已完成。

## 当前阶段与 Zeek 闭环后续顺序（2026-09-27）

~~宏观上仍处于 D1 数据底座实施阶段。~~ **D1 已于 2026-10-01 正式画勾**（四项证据齐备，画勾记录见「D1 的完成定义」一节），当前进入 D1.5（来源与运行治理）与 D2（分析主体）并行阶段。阶段 1 已完成，A03 于 2026-09-28 定版当前 50 GiB 单节点容量边界：只允许 Zeek `conn/dns/http/ssl`，Kafka Topic 保留 24h（2026-09-30 起下调为 12h，见 COL-07b 清理记录），ES Raw/domain/Quarantine 最多保留 7 个 UTC 日分区，根盘 70/75/80% 分别为 warning/critical/停止新增写入水位。O05 的 retention guard、Prometheus、Grafana、主机与 Kafka lag metrics 已部署；邮件外发按用户要求暂缓。248 数据面自 2026-09-30 起由**产品 Launcher**（`tuba-launcher`）统一监督：六个 Zeek 组件、四个 tenant_a 组件与 api 共 11 个服务，manifest 为 `/etc/tuba/tuba-services.json`。切换前它们由 `manage_zeek_live_pipeline.py` 与 `tenant_a_chain.py` 两个 Python 监督器分别管理（见 O01 的 2b 切换记录）。source-adapter 短暂 Fetch/offset commit 错误改为有界退避重试，现场验证子进程强杀后自动恢复。2026-09-28 现场快照曾为六进程 running、source-adapter readiness=200、Prometheus 6/6 targets UP、Kafka lag=0；2026-09-29 只读复核六进程仍 running、Prometheus healthy，lag 抽样出现 TLS=1 和 network=9 的瞬时非零值，仍需观察趋势与索引新鲜度，不能沿用旧的 lag=0 作为当前状态。详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。O01/O04 的 Launcher、安装包、配置校验、依赖探针和有界批量已落地；~~目标主机重启恢复和~~169 非管理员实际启动/停止仍未完成（目标主机重启恢复已于 2026-10-01 完成两次真实验收，见 O04 执行记录）。Zeek 真实纵向链路已接通；这不代表阶段 2 以后或完整业务闭环完成。

1. **先处理监控发现的当前积压并完成可靠性**：248 曾部署 `start-components` 单组件恢复路径和六个 worker 的轻量监督器，异常退出按 2、4、8…秒退避，最大 60 秒，稳定运行 5 分钟后重置；`--restart start-components <component...>` 可将存活直启 worker 安全迁移。**（2026-09-30 起该轻量监督器已被产品 Launcher 取代：退避参数改为 1→30 秒，恢复路径改为 `tuba-launcher start/stop/restart`（2026-10-01 起支持 `--service` 单服务粒度，见 O01 接管后遗留第 4 条），不再有独立监督器的单组件入口；两个 Python 监督器脚本保留在磁盘上仅作回滚退路。见 O01。）**沿用原 adapter token 与 consumer group suffix，没有重置 offset。source-adapter Kafka Fetch/offset commit 增加有界退避重试。邮件接收地址 `1096429536@qq.com` 已指定，按用户要求暂缓 SMTP/Alertmanager 配置。2026-09-29 已在 21 部署按 Filebeat registry WAL/snapshot 的稳定 cursor 证明归档 stage 达到 EOF 后才回收；现场核验 144 个过期文件中先回收 36 个已确认的 conn/dns 文件，其余 108 个（约 16.8 MB）保留。248 当前六个 worker 与 6/6 scrape targets 正常；本轮部署稳定源位置/重复 receipt 兼容更新，并保持既有 consumer group 和 offsets。当前 lag 为流动值，最近抽样总量 4–8，短时主要在 DNS/network 标准索引组；需要继续观察追赶和索引新鲜度。下一步完成 COL-03、COL-07、V02 故障矩阵：隔离 Kafka 故障/恢复、目标机重启、轮转、磁盘满、Topic 重建、积压期间凭据/配置切换、真实跨 offset 重发及归档 spool 回放。不得仅凭“超过 6 小时”删除未确认归档。
2. **随后完成接入治理**：推进 COL-06 的过滤策略版本、影子计数、发布与回滚；推进 C05 的 Topic retention、ACL、分区及消费组定版。当前 Zeek 仅按四个 dataset 与最近 90 分钟归档窗口选择，尚无语义过滤。
3. **再定 UIM 质量规则**：在 N03、C02 中评审缺 SNI 的 TLS、缺 query 的 DNS、缺 host 的 HTTP 是否可以标记为 `partial`。目前原文留在 Raw，标准化结果进入 quarantine。首轮误读 gzip 的 4,295 条及旧 DLQ 423 条保留为历史证据，清理或隔离须先形成策略，不直接删除。
4. **并行准备统一部署管理**：推进 COL-08、COL-09，交付 Management Agent、组件监督、升级/回滚及主机重启恢复。**注意：Zeek 独立实例自 2026-09-30 起已由产品 Launcher 接管（统一起停/状态/日志），但"组件监督"这一项只能算部分完成**——COL-08/COL-09 真正缺的是 Management Agent、升级/回滚，这两项没有因切换而关闭；~~Launcher 按设计不注册 systemd，248 重启后不会自动拉起数据面，当前甚至没有受控的启动入口~~ 主机重启恢复已于 2026-10-01 由 `@reboot /opt/tuba/bin/tuba-boot`（仓库副本 `scripts/tuba-boot.sh`）真实验收通过（见 O04 执行记录；共享 PG 仍需人工/对侧恢复）。
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
      1. ~~**开机恢复仍缺——当前最大的可用性缺口**~~（**已补受控入口，未经重启验收**，2026-09-30）：Launcher 按设计不注册 systemd，此前 248 重启后不会自动拉起数据面。已安装受控开机入口：root crontab 一行 `@reboot /opt/tuba/bin/tuba-boot`（保留既有 crontab 条目；248 的 crond 为 active+enabled），脚本（0700 root）写日志到 `/var/log/tuba/boot.log` 后调用 `tuba-launcher start --manifest /etc/tuba/tuba-services.json`；幂等靠先 `status` 探测（实测 `start` 在已运行时以 `already running` 退出 1，非幂等）。手动执行验证通过：运行中执行 no-op、11/11 仍 running。**剩余风险**：~~未经真实重启验收~~（**已于 2026-10-01 完成两次真实重启验收，见 O04 重启验收记录**；首轮暴露并修复了 tuba-boot 幂等探测误信陈旧 state 的缺陷与 29292 验证 broker 无开机入口两个真实缺陷）。后续可用 Management Agent（COL-08/COL-09）取代该 cron 入口。
      2. ~~**非 root 转换未做**~~（**已执行**，2026-10-01 维护窗口）：原状为 `tuba` 账号已建（uid=967、nologin）但 `/opt/tuba/collector-live` 是 `0700 root`、Launcher 以 root 运行。已按 [NONROOT-CONVERSION-248.md](NONROOT-CONVERSION-248.md) 执行：放权（`collector-live` 0750 root:tuba、`pipeline` 整树 root:tuba 0750/0640、`/opt/tuba/bin` 组件二进制 root:tuba 0750；`kafka/`、`capacity-guard/`、`/etc/tuba/*`、`/var/lib/tuba`、`/var/log/tuba*` 不动；`tuba-launcher` 收紧为 0750 root:root），11 个数据面服务清单 command 套 `/usr/bin/setpriv --reuid=967 --regid=965 --clear-groups --no-new-privs`（validate 预检通过），整份清单重启一次（停机约 10 秒，05:51:19–29Z）且同窗口把数据面 Launcher 主进程换到已就位的新二进制（sha256 `750947ad…f5a4`，pid 1997065，`service_control: true`）。验收：11/11 全部 uid=967 且 NoNewPrivs=1、Launcher 仍 root、全部 loopback 端口在位、active 消费组 lag=0、ES 计数恢复增长、DLQ 三 topic 零新增、Prometheus 6/6 UP、tuba 对 `tuba.env`/`secrets.json`/`tuba-monitoring.env` 仍无访问；稳定 10 分钟 restarts 全 0。权限快照与清单归档于 `21:/opt/tuba-backup/248/20261001-window/`；回滚路径（还原 `tuba-services.json.pre-nonroot-20261001`）保留。监控栈清单未动，capacity-guard 保持 root。
      3. ~~**Launcher 二进制不在包/校验链里**~~（**已纳入包/SHA-256 审计链**，2026-09-30）：248 保持扁平布局（`/opt/tuba/bin`、`collector-live/pipeline/bin`）是**已定的方向**（安装器对齐现实，不重装 248），所以"未收敛到 `releases/<version>`＋`current`"不是缺陷。原缺口是 `/opt/tuba/bin/tuba-launcher` 手工 `install`、不来自任何包。已闭环：`tuba-launcher` 本就在 `scripts/package_tuba.ps1` 的组件清单内，用该脚本从仓库 HEAD（`e63eec5`，Launcher 源码最后改动为 2026-09-28 `286019c`）构建 Linux 包 `dist/tuba-2026.09.30-launcher-linux-amd64.tar.gz`（包级 SHA-256 `d19fba2c…df3e2`，sidecar 同目录，25 文件 allowlist 审计通过）；包内 `bin/tuba-launcher` SHA-256 `d924d656…3e4cf0`。等价性判定：线上手工二进制内嵌 `vcs.revision=dc2f1e5`（`vcs.modified=false`，go1.27.1），`dc2f1e5..e63eec5` 之间 Launcher 及其 Go 依赖**零改动**（仅 docs 与 Python 脚本），故新构建与线上功能等价，差异仅来自 vcs 戳。部署：旧二进制归档至 `21:/opt/tuba-backup/248/cleanup-20260930/bin/tuba-launcher.manual-20260930`（归档后 sha256 复核 `0f719e56…` 与线上一致）；新二进制先放 `tuba-launcher.new` 预跑 `status` 通过后才 `mv` 替换，并写入 `/opt/tuba/bin/tuba-launcher.sha256`（单行 `<hash>  tuba-launcher`，0640 root，与安装器 sidecar 同格式），`sha256sum -c` 通过。替换只换磁盘文件，未重启 Launcher 主进程（pid 1656495 不变）或任何数据面服务；替换后新二进制 `status` 实测 11/11 running、`restarts=0`。注：本机构建时发现 `package_tuba.ps1` 的 Linux 打包会被 Git Bash 的 GNU tar 劫持（`C:` 被当作远程主机），需在 PATH 前置 `C:\Windows\System32` 用 bsdtar——脚本行为本身未改。
      4. ~~**没有单服务回滚粒度**~~（**代码已实现并部署，监控栈实例已实测；数据面待主进程重启窗口**，2026-10-01）：Launcher 的 `start`/`stop`/`restart` 已支持 `--service NAME`（可重复，`status` 同样支持按服务过滤）；不带 `--service` 时保持原 manifest-wide 行为。语义：`stop --service` 在 state 目录写持久期望状态标记 `<name>.stop-request`，runService 循环尊重它——服务优雅停止后**不会被自动拉起，标记跨主进程重启与主机重启保留**，直到 `start --service` 移除；`restart --service` 经瞬态 `<name>.restart-request` 由监督器立即重启目标（不占 crash 退避计数、不增 `restarts`），对已停止的服务等价于 start。fail-closed 能力门：state 新增 `service_control` 位，仅新 supervisor 写入；老 supervisor 在跑时单服务操作明确报错拒绝且不写标记（否则标记会被老 supervisor 忽略、日后新 supervisor 启动时突然生效）。监督器重启**不接管**既有子进程（无 PID 领养设计），重启主进程等于整份清单重启。测试：`internal/launcher` 新增 5 个用例（stop 后不被拉起、restart 只影响目标、stop 跨 supervisor 重启保留、老 supervisor 拒绝、status 过滤），全量 `go test ./...` 26 包 0 FAIL、`go vet` 干净。部署：新二进制 SHA-256 `750947ad…f5a4`（sidecar 已更新、`sha256sum -c` 通过），旧二进制（`d924d656…`）归档 `21:/opt/tuba-backup/248/cleanup-20260930/bin/tuba-launcher.pre-per-service-20261001`（归档后 sha256 复核一致）。监控栈实例已整体换为新 supervisor（pid 1876040→1989059，监控中断约 1 分钟，setpriv 身份逐 uid 不变）并实测 node-exporter：`stop --service` 后旧 PID 退出、40 秒（超最大退避）未被拉起、其余 4 组件 pid/restarts 未动；`start --service` 新 PID 且身份仍为 tuba-node-exporter；`restart --service` 换新 PID、`restarts` 仍为 0；全程数据面 11 服务 status 逐字节不变，结束后 6/6 targets UP。数据面负向实测：`stop --service zeek-ingest` 被能力门正确拒绝（报错指明需重启主进程、无标记落盘、zeek-ingest 未受影响）。**遗留**：~~数据面清单的 Launcher 主进程（pid 1842144，2026-10-01 02:48Z 起）仍是旧映像，重启它等于 manifest-wide 重启 11 个数据面服务（红线），故数据面单服务操作暂被能力门拒绝；待用户批准的低峰窗口重启该主进程后生效。~~ **已于 2026-10-01 维护窗口关闭**：数据面主进程随非 root 转换整清单重启换为新映像（pid 1997065→重启后 2508，`service_control: true`）；实测 `status --service` 过滤与 `restart --service zeek-raw-indexer`（换新 PID、`restarts` 不增、uid 仍为 967）均正常。
      5. ~~**`/opt/tuba/start.sh` 仍是地雷**~~（**已废止**，2026-09-30）：原脚本会 source 语义已变的 `/etc/tuba/tuba.env`，起一个缺 `ES_URL` 的 api。已在 248 上改名为 `/opt/tuba/start.sh.retired`（0600 root，不可执行，仅留档），改名后 `tuba-launcher status` 确认 11/11 running 不受影响；RUNBOOK 警告已同步更新。
      6. ~~**预检与供给守卫仍未在 248 真实库上跑过**~~（**已在 248 真实 PG/Kafka/ES 上实测**，2026-09-30）：248 无 `psql` 客户端，改用同机 `/opt/adms/postgresql/bin/psql`（共享实例另一产品侧自带的客户端），把 `scripts/`+`migrations/` 复制到 248 临时目录后直接对真实库跑 `check_tuba_prerequisites.sh`（`DATABASE_MIGRATION_URL` 取自 `/etc/tuba/tuba.env`，运行时角色按 248 真实现状取 `TUBA_RUNTIME_DB_USER=tuba`——11 个服务全部以 schema owner `tuba` 连接，全部 33 张 public 表属 `tuba`）。**真实判定输出（4 FAIL，exit 1 拒绝）**：① 共用库守卫触发——`public contains unrelated tables (schema_migrations,tuba_schema_migrations)`（两张均为迁移簿记表、不在 `migrations/*.sql` 产物清单内，守卫按定义 fail-closed）；② 运行角色即 schema owner 守卫触发——`runtime role tuba owns 33 table(s), so the DML-only boundary does not hold`；③ Keycloak FAIL 属环境性（248 未部署 Keycloak，`KEYCLOAK_URL` 未设置）；④ host FAIL——`/var/lib/tuba` 仅剩 25%（低于 30% 下限，真实容量信号）。其余 PASS：PG 14.23 可连、31/31 TUBA 表在位、Kafka `10.6.68.248:29292` 可达、ES green 1 数据节点且水位非默认（75%/78%，即 capacity guard 已调过）。**只读验证**：在 `PATH` 前置一个记录全部参数（口令打码）再 exec 真实 psql 的 shim，预检全程只发出 6 条语句（`SELECT 1`、`SHOW server_version`、3 条 `pg_tables`/`pg_roles`/`pg_auth_members` SELECT、1 条 `pg_class` owner 计数 SELECT），与 fake 测试断言的只读语句集形状一致；运行前后 `pg_roles` 与 `public` 表清单的 md5 不变。**供给守卫**（`provision_postgres_runtime_role.sh`，只跑到守卫判定）：默认配置在共用库守卫拒绝（exit 1，列出 `schema_migrations,tuba_schema_migrations`）；仅加 `TUBA_ADOPT_SHARED_DATABASE=yes` 后在第二道守卫拒绝（`tuba owns 33 table(s)... DML-only boundary`）；两轮 shim 日志确认只发出 4 条 SELECT、零 DDL/REVOKE，运行后角色/表 md5 仍不变——拒绝发生在任何写操作之前。**Kafka SCRAM 顺带实测**：用 `/opt/adms/kafka/bin/kafka-topics.sh --list`（admin.properties）只读列出 37 个 topic（zeek/tenant_a 各链 + `tuba.source.ctx_*`），认证有效。全程未干扰数据面：结束后 `tuba-launcher status` 11/11 running、`restarts=0`；临时目录已清理。注：`pg_stat_database` 的 xact/tup 计数器在窗口内有变化，但那是 11 个在线服务自身的写入，不能归因于预检，故只读证据以 shim 语句日志 + 角色/表 md5 为准。
      7. ~~**监控栈不在清单内**~~（**已纳管**，2026-10-01）：prometheus/grafana/node_exporter/kafka_exporter/capacity-guard 此前由 `manage_tuba_monitoring.py` 手动启停、独立于 Launcher，重启后不自恢复。已并入 Launcher 监督——**以第二份清单 `/etc/tuba/tuba-monitoring.json` 由第二个 tuba-launcher 实例管理**，而非并入主清单：`merge-manifest` 的语义是版本升级时从新旧 example 清单差集补服务（见 `internal/launcher/manifest_merge.go`），不适用于运维侧附加栈；而直接改主清单要求 Launcher 全量 stop/start，会连带重启 11 个数据面服务，越过"数据面不受影响"的红线。两份清单各有独立 state_dir（`/var/lib/tuba/launcher-monitoring`）与 log_dir（`/var/log/tuba-monitoring`），密钥单列 `/etc/tuba/tuba-monitoring.env`（0600 root，仅 kafka_exporter SCRAM 口令与 Grafana admin 口令两项 `${...}` 引用，取自既有 secrets 存储）。**逐项身份决策**：Launcher 不支持 per-service 用户切换（子进程继承 Launcher 的 root），直接纳管会让四个专用 nologin 用户（tuba-prometheus/node-exporter/kafka-exporter/grafana）倒退为 root——故四个组件以 `/usr/bin/setpriv --reuid=<uid> --regid=<gid> --clear-groups --no-new-privs` 包装为清单 command（exec 语义，PID 与 SIGTERM 直达服务，身份与切换前逐 uid 一致）；capacity-guard 切换前即以 root 运行且需写 ES 集群设置，保持 root。切换顺序：先写清单+env 并 `tuba-launcher validate`（services=5 通过），再停旧进程（旧管理器 stop 全部回收，无残留）、由 Launcher 接管，监控中断约数十秒；prometheus 的 `--storage.tsdb.path` 指向原目录，历史连续（`up offset 2d` 有数）。**切换暴露并修复一个潜在缺陷**：`rules/kafka.yml` 的 `TubaKafkaConsumerInactiveWithBacklog` 表达式括号不配对（`...})) > 0)` 多一层），旧 prometheus 自 09-28 起从未 reload 过 09-30 编辑后的规则文件，故该告警（含 tenant_a 组）**实际从未生效**；已修（仓库 `deploy/observability/single-node/rules/kafka.yml` 与 248 同步，promtool check 通过），重启后 10 条规则全部加载。**验收**：5/5 running、6/6 scrape targets UP、`kafka_consumergroup_lag` 19 条序列、guard ready=200、grafana /login=200、数据面 11 服务 pid/restarts 全程未动；旧 `manage_tuba_monitoring.py` 与 `manage_tuba_capacity_guard.py` 保留为回滚退路（其 log-rotator 退役，日志由 Launcher 16MiB×3 轮转）。`@reboot tuba-boot` 已改为对两份清单逐一幂等拉起（手动执行 no-op 验证通过），248 重启后监控栈随数据面一并自恢复（重启验收本身仍按用户决定未做）。操作前已将 `/etc/tuba/tuba-services.json` 归档至 `21:/opt/tuba-backup/248/20261001-monitoring-cutover/`（同批归档新清单与 tuba-boot）。
      8. **未做持续稳定性窗口**：验收只有分钟级观察（11/11 running、`restarts=0`、33 个消费组不变、各域文档数上涨）；跨天趋势、跨 offset 重放对账与故障注入（COL-07/V02）仍未做。**（2026-10-01 起已建每日 09:23 定时观察任务，逐日记录如下；跨 offset 重放对账仍未做。）**
        - **2026-10-01（第 1 天）**：Launcher pid=1782707（9-30 16:46Z 起）11/11 running、全部 `restarts=0`；根盘 62%（28G→27G，清理后 Kafka 12h 保留生效，Kafka 数据目录 4.0G→2.6G，磁盘不升反降）；9 路 source-adapter 消费组 lag 全 0（含 3 个无后缀组——此前 RUNBOOK 记录的孤儿冻结 lag 已不可见/已归零，待复核孤儿是否被清理）；ES green 单节点，域计数持续增长：network-zeek 1,450,492、raw-zeek 2,209,739、dns-zeek 383,989、web-zeek 206,555、tls-zeek 17,377、quarantine-zeek 153,916、authentication-tenant_a 17,210、raw-tenant_a 35,390；索引新鲜度秒级（network-zeek 与 authentication-tenant_a 最新 `@timestamp` 距采集时刻 <15s）。**无异常**。注：248 本机 Kafka 工具在 `/opt/adms/kafka/bin`（`collector-live/kafka` 只有配置与数据），命令需 `JAVA_HOME=/opt/adms/adms-jdk`。
        - **2026-10-02（第 2 天，含 10-01 重启窗口后）**：数据面 11/11 + 监控 5/5 running（Launcher pid=2508/2629，均为 10-01 07:07Z 第二次重启后拉起）；`restarts` 计数——api=7（历史累积，含窗口内操作）、capacity-guard=7 与 kafka-exporter=4（O04 verify 已确认的 PG/broker 就绪前依赖退避，原因已知、非新异常）、两个 indexer=1（10-01 单服务部署操作）、ingest/source-adapter=0；9 路 source-adapter 组 lag 全 0；根盘 **67%**（62%→67%，Kafka 数据目录 2.6G→3.7G，12h 保留下预计趋稳，继续盯趋势，>70% 触发处置）；ES green，域计数持续增长：raw-zeek 2,563,850（+354k）、network-zeek 1,685,203（+235k）、dns-zeek 448,152、web-zeek 240,485、tls-zeek 19,744、quarantine-zeek 172,860、authentication-tenant_a 24,549（+7.3k）、raw-tenant_a 50,220（+14.8k）；索引新鲜度秒级（network-zeek 最新 `@timestamp` 与采集同时刻）。**无异常，磁盘趋势需继续观察**。
      9. ~~**生成清单的工具不在版本控制里**~~ **（已收编）**：`gen248.py`（从 `/proc` 推导 manifest＋env 的生成器，即本次修复白名单缺陷的那个文件）原先位于 `.runtime/`、被 `.gitignore` 排除，没有 review、历史或测试。现已移入版本控制为 `scripts/gen248.py`（内容不变），两条守卫的变异用例已固化为 `scripts/test_gen248.py`（`python scripts/test_gen248.py`，7 例）：共享密钥值漂移拒绝写盘及一致值只落地一次的正例；"live 变量到不了任何服务则拒绝写盘"守卫——含加宽 `AMBIENT_DENY` 而不动 `MAY_DROP` 的变异（模拟 `ES_URL` 被静默丢弃）必须触发拒绝、`MAY_DROP` 内变量（`PWD`/`HOME`）被丢弃不触发、以及守卫与拷贝循环的名字正则必须不同、`MAY_DROP` 必须是独立第二份拷贝的结构性断言；外加两服务最小 fixture 的 happy-path（wildcard 监听进 `dropped_listeners`、每服务密钥变 `${TUBA_<NAME>_...}`、共享密钥变 `${VAR}` 引用）。
    - **同批结转的待办**（不在本次切换范围内，但切换后仍未关闭）：COL-07/V02 破坏性故障注入子集——**已于 2026-09-30 执行 6 项中的 5 项**（磁盘满/Kafka 故障/PG 故障/Topic 重建/积压换凭据，见 COL-07b 五条记录，全过且暴露 adapter 对 topic 重建不自愈的缺陷——已于 2026-10-01 修复并复验闭环，见 4/5 修复记录），仅剩目标机重启（用户已决定本轮不做）；Adapter/DLQ 端到端验收测试——用户推迟到后续会话。
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
- [x] O04（G）实现 live/ready、依赖重连、优雅停止、有界队列、配置校验；修复盘点确认的启动配置问题。进行中（2026-09-28）：`internal/config.Load` 对速率、批大小、批字节数、重试次数、等待/退避时长执行范围校验，非法值不再静默回退；API ready 检查 PostgreSQL/Elasticsearch，ingest ready 检查 PostgreSQL/Kafka broker；raw/normalizer/standard/quarantine indexer、control worker、analysis sink、source adapter 已接入独立 loopback 运行探针，ready 随 worker 生命周期变化；standard indexer 增加 16 MiB 默认/64 MiB 上限的批字节预算，Kafka 消息 offset 不提前提交；Go 入口统一接入 Unix SIGTERM 与 Windows Interrupt 生命周期上下文。新增 API/ingest 30 秒请求 deadline、HTTP 读写/空闲超时，以及所有运行时 PostgreSQL pool 的 15 秒 statement timeout（1 秒至 5 分钟）和 5 秒 ping 上限。逐消息同步的 raw/normalizer/quarantine/analysis worker 不维护应用内积压队列，standard indexer 单批同时受条数和字节预算约束。PG/Kafka 依赖断连恢复已有隔离运行证据。修复 API/ingest 的停机竞态：新增 `internal/lifecycle.ServeHTTP`，等待活动 handler 完成后才返回，超时强制关闭连接；API 与 ingest 统一使用该入口。生命周期测试覆盖活动请求 graceful drain、超时强制关闭与监听失败；`go test ./...` 通过。新增真实 Raw、Quarantine、Standard Indexer、Analysis Sink、API 隔离运行验收：服务写入/查询后 offset 与租户检查符合预期，平台中断信号触发正常退出。2026-09-28 后续已验证 Launcher `run` 通过 manifest 监督真实 control-worker：PG/Kafka 断连恢复时 readiness 为 200→503→200，强制结束一次隔离子进程后 Launcher 自动重启 worker；向 Launcher 发送平台中断信号后产品子进程优雅退出，独立 status 确认为 stopped。~~仍缺目标主机重启恢复验收；此前被拦截的 manifest `stop` 命令未重试。~~ **本项勾选（2026-10-01）**：目标主机重启恢复已于当日完成两次真实整机重启验收（见下方「O04 目标机重启验收执行记录」）；manifest `stop`/`start` 已于同日在 248 真实数据面清单上执行（非 root 转换的停/启），`stop` 在 10 秒内优雅回收 11 个服务。
- O04 目标机重启验收执行记录（2026-10-01 维护窗口，两次真实整机重启）：**已执行并通过，取代此前的"预案待执行"状态。** 第一次重启（06:03:56Z 发起，boot 完成约 06:38Z——**关机阶段耗时约 34 分钟**，原因未查明，journald 无持久日志；第二次重启仅约 1.5 分钟，未复现）暴露两个真实缺陷并已当场修复：
  1. **tuba-boot 幂等探测误信陈旧 state**：`tuba-launcher status` 在陈旧 state 文件下仍退出 0 且逐服务打印残留 "running" 行，tuba-boot 据此判 "already supervised" 而什么都不做——首轮重启后两份清单均未拉起。修复：tuba-boot 改为正证据门槛（grep `^TUBA launcher pid=[0-9]`）；同时修复本仓库 `scripts/o04_reboot_acceptance.sh` 的同类假阳性（`parse_launcher_status` 现在先查头部行，无活 pid 即判 supervisor-not-running——修复前首轮 verify 曾误报 11/11 running）。~~**Launcher 代码层面的遗留缺陷**：`status` 在 stale state 下应退出非零且不应打印陈旧行，待后续代码修复。~~ **已修复并部署（2026-10-01）**：`internal/launcher` 的 `statusTo` 现在在 supervisor 不存活（pid 不存在、pid 被复用致 `runner_identity` 不匹配、或 state 来自上一 boot——Linux 身份即 boot ID+`/proc/<pid>/stat` 启动时刻）时输出 `TUBA launcher has no live supervisor: ... (stale state from boot <boot_id>)` 并**返回错误（exit 1）、不再打印任何陈旧的 per-service 行**；活 supervisor 路径输出不变。新增测试 `TestStatusFailsOnStaleState`（伪造他 boot 身份/死 pid 两子用例，断言非零返回、无 running 行）与 `TestStatusReportsLiveSupervisor`（活 supervisor 仍 exit 0 且输出完整）；`go build`/`go vet` 干净，`go test ./internal/launcher` 通过（注：本机 Windows 全量跑时 `TestStoppedServiceSurvivesSupervisorRestart` 的 TempDir 清理偶发文件占用失败，为预存 flake，未改代码的 stash 全量跑同样复现，单跑与逻辑断言均通过）。部署：linux/amd64 新二进制 SHA-256 `dc1f3e08…70d0`，旧二进制（`750947ad…f5a4`）归档 `21:/opt/tuba-backup/248/cleanup-20260930/bin/tuba-launcher.pre-stale-status-fix-20261001`（归档后 sha256 复核一致）；先以 `tuba-launcher.new` 对两份清单跑 `status` 与旧版**逐字节一致**后才 `mv` 替换，sidecar 已更新、`sha256sum -c` 通过；伪造 stale state 实测旧版 exit 0 且打印陈旧 running 行、新版 exit 1 且无陈旧行。**只换磁盘文件，两个 Launcher 主进程（pid 2508/2629）与全部 16 个服务未动**；status 是独立调用，新行为即时生效，supervisor 本体的新二进制待下次主进程启动生效。tuba-boot 的正证据门槛保持有效且仍正确（幂等双保险）。
  2. **TUBA 验证 Kafka broker（10.6.68.248:29292）无开机入口**：首轮重启后 broker 未起，消费组/DLQ 查询全空。tuba-boot 已增加幂等 broker 拉起段（端口探测后 `kafka-server-start.sh -daemon`，`JAVA_HOME=/opt/adms/adms-jdk`）。tuba-boot 当前版本收编进仓库 `scripts/tuba-boot.sh`，旧版本归档 `21:/opt/tuba-backup/248/20261001-window/`。
  第二次重启（07:07:05Z 发起，SSH 07:08:24Z 恢复）：**tuba-boot 全自动拉起 broker + 两份清单（16 个服务），无需人工介入 TUBA 侧**。唯一人工步骤：共享 PostgreSQL（另一产品 adms 资产）因其自身 daemon 状态闭锁（`STATUS_FILE=1`）未自启，用 `pg_ctl` 手动拉起（干净 shutdown、无恢复异常）——**这是外部依赖的重启恢复缺口，不属于 TUBA 组件，但 TUBA 的无人值守恢复事实上受它阻塞**，已通知性质地记录在 RUNBOOK。最终 verify（修复后解析器）：PASS=10 WARN=2 FAIL=0——11/11+5/5 running、消费组集合与基线逐字一致（33 个）、活跃组 lag 追平（345s）、DLQ 零新增、ES 计数 ≥ 基线且恢复增长、根盘 64%；两条 WARN 均为重启计数（api 7 次、capacity-guard 7 次等），原因确认为是 PG/broker 就绪前的依赖顺序退避，非持续崩溃。证据：`.runtime/o04-reboot/window-20261001`（gitignored）含基线与 verify 输出。
  原预案条目（2026-10-01 早）：新增 `scripts/o04_reboot_acceptance.sh`（precheck/baseline/verify 三阶段，全程只读），干跑验证通过；验收步骤见 [RUNBOOK](RUNBOOK.md)「O04 主机重启验收步骤」。
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
  - **2026-10-01 可查询性验收（D1 画勾证据第 4 条，只读查询，未改任何配置）**：
    1. **容量保护状态**：capacity guard `127.0.0.1:19100` live/ready 均 200；`/metrics` 实测 `tuba_capacity_disk_used_percent 62.117`、`tuba_capacity_level{normal} 1`、`tuba_capacity_delete_candidates 0`、`tuba_capacity_deleted_indices_total 6`（60 秒周期的删除计数持久可查）。ES 当前水位读回：**low=75% / high=78% / flood_stage=80%**（`_cluster/settings?flat_settings=true`，与 capacity guard 写入值一致）；`_cat/allocation` 当前 disk.percent=62。COL-07b 1/5 实测过 86% 时 48/48 索引加 `read_only_allow_delete` 块、恢复后 ≤30 秒解除。
    2. **Kafka 消息过期**：`kafka-configs.sh --describe` 读回 `tuba.collector.zeek_validation_20260927_001.raw.v1` 及全部三个 DLQ topic 均为 `retention.ms=43200000`，synonyms=`DYNAMIC_TOPIC_CONFIG`（即 12h 动态配置真实生效，非默认值）。**顺带发现一个偏差**：2026-09-30 清理把 12h 应用到了**全部** topic，覆盖了 DLQ 原来的 24h（86400000）——DLQ 证据窗口因此减半为 12h，文档「已知数据缺口」一节写的 24h 已不再成立。**2026-10-01 补记：该偏差已修复**——三个 DLQ topic（`tuba.collector.tenant_a.dlq.v1`、`…zeek_validation_20260927_001.dlq.v1`、`…source-adapter.dlq.v1`）的 `retention.ms` 已恢复为 `86400000`，经 `kafka-configs.sh --describe` 逐条读回确认；被 12h 窗口滚空的历史存量不可恢复（见下条 earliest==latest 实测），恢复只影响此后的新隔离记录。
    3. **DLQ 内容与原因聚合**：三个 DLQ topic 当前实测 earliest==latest（tenant_a 509/509、zeek 9565/9565、source-adapter 10470/10470）——**9-30 的存量已被 12h 保留滚空，当前为零记录**，与「出口期限」一节的预言一致。DLQ 消息格式为 `internal/deadletter` Envelope（`failure.stage/code/message/retryable/attempts` + `source.topic/partition/offset`），可按 `failure.code` 聚合；本次一次性新消费组 + `--from-beginning --timeout-ms` 只读消费验证了查询通道本身可用（返回 0 条，未建常驻消费者）。
    4. **永久拒绝的持久可查面在 ES Quarantine，不在 DLQ**：对 `logs-ueba.quarantine-zeek_validation_20260927_001` alias 做 terms 聚合实测——总数 154,574（当前保留窗口），按 stage：UIM 132,848 / DIP 21,726；按 code：`UIM_TLS_SERVER_NAME_REQUIRED` 120,374、`PAYLOAD_HASH_MISMATCH` 17,429、`UIM_DNS_QUERY_REQUIRED` 6,557、`UIM_HTTP_HOST_REQUIRED` 5,917、`DIP_ZEEK_ORIGINAL_INVALID` 4,295、`DIP_ZEEK_DATASET_MISMATCH` 2；`logs-ueba.quarantine-tenant_a`：509 条全为 `PAYLOAD_HASH_MISMATCH`。文档字段含 `raw_event_id`/`stage`/`code`/`occurred_at`，单条可溯源。
    5. **ES 过期保护的实际机制**：`tuba-v1-*` 日索引**没有挂 ILM**（`_settings` 读回无 lifecycle 键）；ES 侧保留由 capacity guard 按"当天+前 6 个 UTC 日"删除实现（当前现存日分区后缀 2026.09.26–2026.10.01，`deleted_indices_total=6` 佐证删除真实发生）。`_ilm/policy` 里存在 `tuba-anomaly-v1`/`tuba-authentication-v1`（均 90 天 delete），但属旧分析链路资产，不是当前 Zeek/tenant_a 数据面的保留机制。
    **结论**：I06 要求的"可查询状态"四类都有实测查询路径（guard metrics / kafka-configs / DLQ 只读消费 / Quarantine ES 聚合）。两处偏差已记录：DLQ 保留被意外降为 12h；ES 保留靠 guard 而非 ILM。「过滤」「归档滞后」的可查询状态（影子计数、归档水位）未在本轮覆盖。本项仍不勾选。

阶段出口：确认接收即有持久消息；Collector 断线/重试不丢采集位置；原始证据独立可追溯。

Collector 细化任务（2026-09-27 已按成熟采集器方案重新定义；旧任务见 [历史清单](history/COLLECTOR-TODO-20260926.md)，历史编号不代表下列任务完成）：

- [x] COL-01（G/D，P0）定版接入合同：Beat 消息 schema、原始证据、来源上下文/Topic 绑定、transport delivery ID、source position、大小限制、三段确认、永久拒绝及重试。初版 schema/样例/Topic 合同已落在 `contracts/events/beat-ingress/1/`；OpenAPI 已引用 Beat 输入 schema，并说明 1 MiB JSON 对象与 adapter 的 Kafka source-position 编码。202 回执返回 source_context_id/source_position/payload_hash，adapter 校验三者后才推进来源 offset。ingest 已增加内部 adapter route，并按规范 Topic ID 从 PG 解析启用 source instance/context；adapter 配置只保留 Topic allowlist。确认 delivery position=`topic/partition/offset`，明确跨 offset 重发不去重。端到端故障验收及完整 OpenAPI 解析/兼容门禁仍未完成。**2026-09-30 复核与对齐：** 合同 `contracts/events/beat-ingress/1/contract.md` 有三处陈述与已验证的实现不符，已就地订正——(1) 状态行原写“Topic ACL 与 adapter 尚未部署”，实际早已部署并闭环；(2) 原文“除非来源适配器另行提供并验证稳定位置，不承诺跨 offset 去重”的条件**已被满足**：稳定位置增强已实现且对 Zeek/Windows 缺失即 400，实测同一记录换 offset+777 重发返回同一条 `receipt_id`、raw topic 中该 `raw_event_id` 只出现 1 次；(3) 原文“profile 准入隔离仍待实现”，实际 ingest 已对两类 profile 做 400 拒绝——**并显式记录了它与原文的差异**：原文写“写接入 quarantine”，实现是“400 → adapter DLQ”，两者都“不发布不可信 Raw”但机制不同，合同已改以实现为准。
  现有关键不变量均有测试覆盖并通过（`go test ./internal/sourceadapter ./internal/ingest ./internal/rawevent`）：ingest 5xx 时 **offset 保持未提交**（`TestProcessUntilCommittedRetriesIngestServerError`）、永久 4xx 才隔离并提交以免卡住分区（`TestProcessUntilCommittedQuarantinesIngestRejectedEvent`）、稳定位置强制且**跨 offset 去重**（`TestWindowsSecurityIngressRequiresStablePositionAndDeduplicatesAcrossOffsets`）、canonical hash 忽略采集器传输元数据（`TestCanonicalPayloadHashIgnoresCollectorLocationMetadata` 等）。
  **OpenAPI 兼容门禁已实现**（按用户决定另开独立脚本，不破坏原校验器的零依赖前提）：新增 `scripts/validate_openapi.py`，用 PyYAML 解析整个文档、**递归解析全部 `$ref`**（内部 JSON 指针解析到已解析的文档，外部 `.json` 引用要求文件存在且**复跑 `validate_contracts.validate_schema` 的同一套规则**）、检查每个 path 的方法与 responses、并要求 `BeatIngressEvent` 仍指向规范 Beat schema。已接入 `make contracts` 与 `make check`，前置依赖写进 DEVELOPMENT.md（与 helm/go/uv/pnpm 同类）。
  门禁自身的证据：正常路径通过（**42 paths、111 条内部引用、2 条外部引用全部解析**）；变异测试逐项确认能拦住漂移——内部引用指向不存在的组件、外部引用指向不存在的文件、Beat 引用被换成别的 schema、某 path 抽掉 responses、某 path 抽掉全部 HTTP 方法、openapi 版本被改，**全部被拦下**。其中一次“把 get 改名”的变异未被拦下，经核查是**变异无效**（该 path 还声明了 post，仍属合法），不是门禁漏洞，已单独用“抽掉全部方法”的变异复验并被拦下。
  **本项可勾选**。范围说明：`端到端故障验收`在此按 D1 定义的非破坏性子集计（见 COL-07/V02——该子集五项已于 2026-09-30 全部完成；破坏性子集归 D4，其中 6 项之 5 已于 2026-09-30 执行通过（见 COL-07b），仅目标机重启按用户决定暂缓，不作为本项前置）。
- [x] COL-02（G/O，P0）锁定 Filebeat/Winlogbeat 版本、OS/架构、Kafka 输出及磁盘队列能力；核对制品与再分发条件，建立带哈希的组件 manifest、下载/离线导入和统一目录包。已固定原型版本 8.19.0、Linux amd64 Filebeat 与 Windows amd64 Filebeat/Winlogbeat，manifest 校验值取自 Elastic 官方 SHA-512 sidecar；`scripts/package_managed_collectors.ps1` 实现下载/缓存/离线校验/打包且未执行。21 上现有 Filebeat 为 8.19.0，官方包授权审查、Beat→Kafka 与 broker/queue 兼容性及真实受管运行仍待验证。
  - **2026-09-30 复核：仍不能勾选，两个缺口且相互关联。**
    - **官方包授权审查没有做。** 现有记录只有一条设计原则（`COLLECTOR-DESIGN.md`：“默认支持从批准地址下载并校验，**不预设可任意重新分发厂商二进制**”），**没有对 Elastic 实际条款的核对结论**。这不是形式问题：`manifest.v1.json` 声明的三个制品都是 Elastic 8.19.0 包，缓存与再分发它们是否被允许直接决定下一步能不能做。
    - **统一目录包没有产出，打包路径从未端到端跑通。** `scripts/package_managed_collectors.ps1` 实现齐全（读 manifest、`-Offline` 校验、暂存 manifest），但 `dist/component-cache` 里**只有 1 个制品**（`winlogbeat-8.19.0-windows-x86_64.zip`），manifest 声明的另外两个（Linux/Windows Filebeat）不在缓存里，`dist/` 下也没有 unified 目录包产物。**顺序上必须先有授权结论，再决定能否下载并缓存这两个制品**——这正是两个缺口关联的地方。
    - 兼容性一项其实有实践证据但未成文：链路在 Kafka 4.3.1 上以 `version: "2.1.0"` 持续投递（Zeek raw 今日 10 万+、tenant_a 23,075），磁盘队列目录在四个实例上都在活动。**受管运行**若指 Management Agent 下发，则属 `COL-08`/D1.5。
  - **2026-10-08 官方包授权审查（前置调研，纯文献核对，未触任何主机）。** 结论：**允许**把 Elastic 官方 Filebeat/Winlogbeat/ES 制品原样随统一目录包再分发给客户部署，需满足三项保留条件（见下）。依据与推理：
    - **许可归属。** Filebeat/Winlogbeat 8.x 的默认发行制品（即 `artifacts.elastic.co` 下载页提供的 tar.gz/zip，manifest 中三个制品均属此类）受 **Elastic License 2.0** 管辖，官方下载页原文：“This default distribution is governed by the Elastic License…”（https://www.elastic.co/downloads/beats/filebeat ）。Beats 未纳入 SSPL/AGPL 三许可体系——SSPL 与 2024-08 新增的 AGPLv3 选项只覆盖 Elasticsearch/Kibana（的相应源码部分，AGPL 自 8.16 起生效）；ES 默认二进制（RPM/Docker 镜像）同样受 ELv2 管辖。**服务器端（ES）与采集端（Beat）在再分发规则上无差别**：默认发行制品都走 ELv2，限制相同。
    - **再分发权。** ELv2 第 1 条 Copyright License 明确授予 “use, copy, **distribute**, make available, and prepare derivative works”（https://www.elastic.co/licensing/elastic-license ）。官方 FAQ 对此有直接场景回答：“I am building an application on top of Elasticsearch… **You may freely use Elasticsearch inside your SaaS or self-managed application, and redistribute it with your application**, provided you follow the three limitations”（https://www.elastic.co/licensing/elastic-license/faq ）。因此“原样转发放进自己产品的组件缓存/统一目录包”落在显式授权范围内。
    - **三条限制对 TUBA 用法的核对。** (1) 不得作为 hosted/managed service 提供给第三方：TUBA 是把 Beat 部署在客户侧、ES 单节点部署在客户环境、客户**不直接访问** ES API——与 FAQ 中两个被允许的示例同型（“contractor setting up Elasticsearch… for my clients to use internally → permitted”；“MSP… if your customers do not access Elasticsearch… permitted”），不构成托管服务。(2) 不得规避 license key 功能：TUBA 只用免费/基础功能（`setup.template/ilm.enabled:false`，无 X-Pack 付费特性），不涉及。(3) 不得删除/隐藏许可与版权声明，且 ELv2 Notices 条款要求**任何获得副本的人同时获得 ELv2 条款文本**：统一目录包内每个 Elastic 制品必须保留其包内自带的 `LICENSE.txt`/`NOTICE.txt`（不解包重打包丢文件），并在 bundle 根随附 Elastic License 2.0 文本；`manifest.v1.json` 已声明 `include_upstream_license_and_notice_files: true`，方向正确，打包脚本需实际核验。
    - **官方渠道补充。** Elastic 无公开的“OEM 强制审批”要求——FAQ 允许随应用再分发，仅对“以托管服务形式向客户直接开放 ES/Kibana 大部分功能”的场景要求联系 elastic_license@elastic.co 商谈。社区常见做法（打包随产品分发与下载页直连两种）均存在；因 TUBA 目标环境离线，随统一目录包分发是合理且被许可允许的路径。若未来形态变为向客户提供 TUBA 托管服务，需重新评估限制 (1)。
    - **剩余动作（授权结论已解除阻塞，按序执行）：** ① 执行 `scripts/package_managed_collectors.ps1` 下载并校验补齐 `dist/component-cache/` 缺失的两个 Filebeat 8.19.0 制品（Linux tar.gz、Windows zip），SHA-512 对官方 sidecar 核验；② 打包脚本增加“制品内 LICENSE/NOTICE 完整 + bundle 根随附 ELv2 文本”的校验步骤；③ 产出统一目录包并端到端跑通打包路径；④ 完成后将 `manifest.v1.json` 的 `public_bundle_redistribution_approved` 置为 true 并记录本结论。
  - **2026-10-08 落地执行（纯本地，未触 248/21）：剩余动作 ①–④ 全部完成，本项勾选。**
    - **① 制品补齐并核验。** 本机可直连 artifacts.elastic.co。三个制品的官方 `.sha512` sidecar 实测值与 `manifest.v1.json` 声明值逐一比对**完全一致**（filebeat linux tar.gz `3c795b14…08a8`、filebeat windows zip `1701c79c…ee39`、winlogbeat windows zip `e1681a70…1a58`）。两个缺失 Filebeat 制品经官方 URL 下载进 `dist/component-cache/`，本地 `sha512sum` 复算三制品全部匹配 manifest。
    - **② 许可校验落地。** ELv2 官方文本取自 Elastic 仓库 `licenses/ELASTIC-LICENSE-2.0.txt`（v8.19.0 tag），存为 `deploy/components/ELASTIC-LICENSE-2.0.txt`。`scripts/package_managed_collectors.ps1` 增加：启动时要求该文件存在；每个制品解压后强制检查包根 `LICENSE.txt`/`NOTICE.txt`，缺失即中止；bundle 根随附 `ELASTIC-LICENSE-2.0.txt`；README 改为声明 ELv2 管辖与随附文本（删除原“授权审查未完成”措辞）。tar.gz 解压改为显式调用 `%SystemRoot%\System32\tar.exe`（bsdtar），规避 GNU tar 劫持 PATH 的坑。
    - **③ 统一目录包产出。** 以 `-Tag v1` 端到端跑通打包路径，产出 `dist/managed-collectors/tuba-managed-collectors-v1.zip`（约 141 MiB）。结构核验符合 manifest 声明：`filebeat/8.19.0/linux-amd64/…`、`filebeat/8.19.0/windows-amd64/…`、`winlogbeat/8.19.0/windows-amd64/…` 三个组件目录，**每个组件根均含 LICENSE.txt 与 NOTICE.txt**（3 组件共 6 个文件）；bundle 根含 `ELASTIC-LICENSE-2.0.txt`、`manifest.v1.json`、`README.txt`。manifest 放行后又以 `-Offline` 模式纯缓存复跑成功，验证离线导入路径可用，且 bundle 内嵌 manifest 即为放行后的版本。
    - **④ manifest 放行。** `deploy/components/manifest.v1.json` 的 `public_bundle_redistribution_approved` 置 `true`，`license_review` 更新为 `approved-2026-10-08-elastic-license-2.0`。
    - **兼容性成文（补 2026-09-30 复核的“有证据未成文”缺口）。** 21 上 Filebeat/Winlogbeat 8.19.0 以 `version: "2.1.0"` 协议向 Kafka 4.3.1（单 broker KRaft）持续投递（Zeek raw 单日 10 万+ 条、tenant_a 23,075 条），四个实例的磁盘队列目录持续活动——Beat→Kafka 协议与 broker/磁盘队列兼容性据此确认。`manifest.v1.json` 的 `compatibility` 段与此一致。
    - **范围说明。** “真实受管运行”（Management Agent 下发配置）按 2026-09-30 复核归属 COL-08/D1.5，不作为本项前置。
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
- [x] COL-06（G/O，P0）实现来源范围配置与 TUBA 过滤策略版本、影子计数、原因码、场景保护及回滚；区分源端过滤与 adapter 准入过滤。验收：无审计计数的源端复杂规则不能发布，平台过滤不能冒称减少源端网络流量。**2026-10-02 完成并勾选。**
  - **实现（adapter 准入过滤的影子计数切片）**：`internal/sourceadapter/filter.go` 定义版本化 `FilterPolicy`（policy_id/version/mode/rules/protected），规则为确定性等值合取（点路径解析同时支持嵌套对象与 Beat 原文的字面点号键如 `id.orig_h`，最长字面键前缀优先），`protected` 为场景保护条件（命中即豁免过滤并单独计数，例如 Windows 1102 清日志事件永不被过滤规则吞掉）。**发布门禁编码在 `Validate`：mode 只接受 `shadow`，任何 enforce/drop 配置直接拒绝**——不存在任何能丢弃事件的代码路径，满足"无审计计数的源端复杂规则不能发布"。adapter 在本地事件校验后、投递 ingest 前求值：命中只增计数器 `tuba_source_adapter_filter_shadow_matches_total{policy_id,version,rule_id,reason}`，保护命中计 `tuba_source_adapter_filter_protected_total{...}`，求值异常计 `filter_evaluation_errors_total`，**事件流零改变**。标签取值全部由校验正则限制在 `internal/telemetry` 扁平渲染可安全表达的字符集（id 只允许小写字母/数字/下划线，因为 Registry 会把 `-` 渲染为 `_`）。策略经 adapter 配置文件内联（`filter` 字段，`DisallowUnknownFields` 解码），版本固定在文件中，**回滚=还原上一版配置并 `restart --service`**（旧配置已归档 21，见下）。`cmd/tuba-source-adapter` 启动日志打印 policy_id/version/mode/规则数。
  - **区分源端与平台过滤**（验收第二条）：影子/准入计数发生在 adapter（接入 Kafka 之后），代码注释与本条记录明确：**它只减少 Raw/ES 侧写入的可能量，不减少来源到接入 Kafka 的网络流量**；Beat 原生过滤继续只用于范围选择与经批准的简单降噪（COLLECTOR-DESIGN §5），源端复杂规则在具备可审计计数前不得发布——目前代码层面根本不存在 enforce 路径。
  - **测试**：`internal/sourceadapter/filter_test.go` 8 项——非 shadow 模式一律拒绝（含 enforce）、8 类畸形策略拒绝、首条命中规则胜出、合取语义、保护场景优先于 catch-all 规则、非 JSON 载荷报错、字面点号键解析，以及两条端到端断言：**影子命中的事件仍完成一次 ingest 投递、一次 offset 提交、零 DLQ**（指标与事件流双重核对）。全仓 `go test ./...` 通过（唯一 FAIL 为 `internal/launcher` 的 `TestStoppedServiceSurvivesSupervisorRestart` Windows TempDir 文件锁清理 flake，在未改动的 master 上复现，与本次无关）；`go build ./...`、`go vet ./...` 无输出。
  - **248 部署与影子证据（2026-10-02）**：受影响组件仅 `zeek-source-adapter` 一个服务——改动不涉及 `rawevent` 哈希/校验语义（adapter 只是 receipt 比对方且该代码未动），**不在 RUNBOOK rawevent 同批部署约束内**。旧二进制（sha256 `774e0547…e47e`）与旧配置归档至 `21:/opt/tuba-backup/248/20261002-col06-shadow-filter/`（归档后 sha256 复核一致）；新二进制 linux/amd64 sha256 `0d6b554e…9f40`（本地构建与 248 落地一致）以 `install -m 0750 -o root -g tuba` 替换，`tuba-launcher restart --manifest /etc/tuba/tuba-services.json --service zeek-source-adapter` 单服务重启，其余 10 服务 pid/restarts 未动。新进程 pid=33906、uid=967（setpriv 语义保持）、`/health/ready`=ready，启动日志含 `filter policy zeek_noise_assessment version 2026.10.02a mode=shadow rules=2 protected=1`。**影子策略**：`dns_ptr`（zeek.dns PTR 反查，NOISE_DNS_PTR）与 `conn_dns_service`（zeek.conn service=dns，NOISE_CONN_DNS），保护规则 `winlog_log_clear`（event.code=1102）。**读回证据**：重启后 90 秒窗口 `events_fetched=events_accepted=offsets_committed=481`（恒等 = 无丢弃无跳过）、`filter_shadow_matches_total` dns_ptr=69 / conn_dns_service=72 且 Prometheus（19090）已抓到带完整标签的序列；`filter_evaluation_errors_total`、`dlq_written_total`、`events_rejected_total` 均未出现（=0）；kafka_exporter 侧六路 source-adapter 消费组 lag 全 0。
  - **范围说明**：本次交付的是影子计数与发布门禁；按设计（代表性时段 shadow→保护场景检查→单来源灰度→逐步执行），enforce 路径在有真实影子数据支撑前**有意不实现**。COL-08 的 Management Agent 配置下发后，策略下发通道应从静态配置文件迁移到版本化下发，属 COL-08 范围。
- [x] COL-07a（G/O，P0）非破坏性可靠性验收（D1；2026-09-30 由 COL-07 拆分）：采集端强杀后重读与补齐、日志轮转不丢记录、重复 offset/跨 offset 重发、归档 spool 回放与确认水位、采集端断网后补齐。**五项全部完成**，逐项证据见下文 2026-09-30 的五条记录（方法、观测量、结论俱全）。最后一项"采集端断网后补齐"于 2026-09-30 21:20–21:24 执行：iptables 精确 REJECT 到 248:29292 的出站 240 秒，Filebeat 磁盘队列峰值约 21 MB，恢复后 61 秒内四路 adapter lag 归 0、ES 各域计数相应增长，无丢失。
- [x] COL-07b（G/O，P0）破坏性故障注入（D4；原 COL-07 的破坏性子集）：磁盘满、Kafka/PG 故障、Topic 重建、目标机重启、积压期间换凭据与配置。**2026-09-30 22:49–23:44 已执行 6 项中的 5 项**（磁盘满 / Kafka 故障 / PG 故障 / Topic 重建 / 积压换凭据），逐项证据见下方 COL-07b 五条记录（1/5–5/5）：全程无数据丢失、DLQ 零新增，三项保护机制按设计触发；新暴露缺陷一条（source-adapter 对 topic 删除重建不自愈，须重启组件恢复，见 4/5）**——该缺陷已于 2026-10-01 修复并在 248 复验通过（见 4/5 修复与复验记录）**。**磁盘阻塞已解除**（2026-09-30 晚间清理把根盘从 75% 降到 64%）。**最后一项目标机重启已于 2026-10-01 完成**（两次真实整机重启，含 tuba-boot 缺陷修复后的一次全无人值守 TUBA 恢复，见「O04 目标机重启验收执行记录」；唯一人工步骤是外部共享 PG 的拉起）。**本项据此勾选。**
  - **2026-09-30 状态：非破坏性子集已完成；破坏性子集 6 项之 5 已于同日晚间执行通过（见 COL-07b），仅目标机重启按用户决定暂缓，因此本项不整项勾选。** 完成的五项非破坏性验收（重复投递与跨 offset 重发、采集端强杀后重读与补齐、日志轮转不丢记录、归档 spool 回放与确认水位、采集端断网后补齐）已逐项留证据，见下文 2026-09-30 的五条记录。**破坏性子集**（磁盘满、Kafka/PG 故障、Topic 重建、积压期间换凭据）的执行依赖磁盘余量——2026-09-30 晚间清理前根盘常态 71%、分配线 75%，只剩 4 个点余量；清理后降到 64%（见下条记录），余量约束解除后当夜即完成五项注入。按 D1 定义，D1 计入的是本项的**非破坏性子集**（五项已于 2026-09-30 全部完成，含断网补齐），破坏性子集归 D4。**拆分已执行（2026-09-30）**：本编号不再作为待决策项存在，见上方 COL-07a／COL-07b；下文各条日期记录保留为两者的共同证据。
  - **2026-09-30 磁盘清理记录（248 根盘 75%→64%，释放约 5G）**：直接删除 `/root/.cache/go-build`（157M）、`/root/admc-analysis-fix`（191M）、`/root/go`（48M）；经逐文件字节数校验一致后归档至 `21:/opt/tuba-backup/248/cleanup-20260930/` 再删本地——`/opt/tuba/bin` 的 5 个陈旧 tuba-api/tuba-source-topic-admin 制品（78M，在用二进制未动）、`/opt/tuba/backups`（218M）、`collector-validation`（90M）、`collector-validation-v2`（119M）、`/opt/tuba/build`（167M）、`/opt/tuba/monitoring/staging`（660M，含唯一件 o05-config.tgz）、`/opt/tuba/packages/elasticsearch-8.19.0-x86_64.rpm`（625M，仓库无同源缓存故归档不直接删）。Kafka 保留由 24h 降为 12h：该版本 KRaft 拒绝动态 broker 级 `log.retention.hours`，改为对全部 topic 动态设 `retention.ms=43200000`（免重启）并同步把 `server.properties` 改为 `log.retention.hours=12`；数据目录 6.5G→4.0G。全程未重启任何数据面服务，清理后 tuba-launcher status 11/11 running restarts=0，九路 source-adapter 消费组 lag 全 0。
  - 已完成其中的单节点 worker 故障切片：PostgreSQL/Kafka 中断恢复、Launcher 托管进程强杀重启和优雅退出均于 2026-09-28 在 71 隔离通过，详见本 TODO 的 O04/COL-07 演练记录。其余 COL-07 场景及 Zeek Collector spool 端到端积压/重复投递演练仍待完成。
  - 2026-09-28 增加本机回归：source-adapter 注入 Kafka Fetch 与 offset commit 短暂失败，验证 retry 后 ACK/offset 顺序保持且只调用一次 ingest receipt；ingest 增加来源 Topic 绑定、可信上下文、重复 receipt 幂等、正文冲突、无效 token 和未绑定 Topic 覆盖。`go test ./...` 与 `go vet ./...` 均通过。248 上六个 Zeek worker 全部由监督器管理，source-adapter 子进程 SIGKILL 后 supervisor 拉起新 PID、readiness 恢复；现场只读复核六进程 running、Prometheus 6/6 targets UP、Kafka lag 0。此证据关闭 worker 单进程崩溃恢复切片，不能替代整机重启、断网、磁盘满、轮转覆盖、Kafka/PG 故障矩阵与 spool 回放验收；详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。
  - 2026-09-29 已把 Filebeat registry 解析接入 `archive-sync`：稳定读取 active snapshot 与 WAL 的 set/remove，cursor 到达 stage 文件 EOF 才列入可回收集合；registry 缺失、格式异常或读取期间变化均 fail-closed。6 项本地 Python 测试和 py_compile 通过。21 上以临时副本只读核对后部署并备份旧脚本，重启的仅为 `archive-sync` 子进程，四个 Filebeat 不间断；归档状态读回证明已确认文件被回收，而未确认文件仍保留。每 30 秒继续执行同一安全规则。详见 [容量与可靠性观测记录](CAPACITY-OBSERVATION-20260927.md)。
  - 2026-09-29 在 21 完成三轮隔离队列测试：2,000 条输出不可达、约 1,919,784 B 持久化后 SIGKILL，恢复后 unique=2,000、duplicates=0；50,000 条将 16 MiB queue 填至 15,999,234 B 后强杀，恢复后 unique=50,000、duplicates=0，未入队数据从输入文件续读。真实 broker 停止/启动轮发送 100,000 条，queue 达 15,999,360 B 后强杀 Filebeat 并恢复同一 Kafka/队列；三次实测均完整收到 100,000 个唯一事件、malformed=0，但重复分别为 15、94、65 条。重复记录的 Filebeat `log.file.device_id`、`log.file.inode`、`log.offset`、正文 hash 和 agent 元数据相同，Kafka delivery offset 不同，证实该故障窗口提供 at-least-once 语义。
  - 2026-09-29 实现并部署 Beat 稳定位置：Filebeat `filebeat-v1:<device_id>:<inode>:<log.offset>`；Winlogbeat `winlogbeat-v1:<hex(computer)>:<hex(channel)>:<record_id>:<UTC timestamp>`。Kafka `topic/partition/offset` 单独作为投递位置；Windows Security 缺稳定位置时拒绝接收。旧 Raw 日索引使用 `dynamic:strict` 且映射不含可选 `delivery_position`，因此继续把投递元数据持久化在 PG receipt 和 Raw Kafka 信封，Raw ES serializer 不写该字段，避免对历史索引做管理员 mapping 迁移。新 `tuba-ingest`、`tuba-source-adapter`、`tuba-raw-indexer`、`tuba-normalizer` 已部署至 248，旧二进制备份于 `/opt/tuba/backups/reliability-20260929/`；原 context、服务 token 与 group suffix 保留，offset 未重置。Go 全量测试、`go vet ./...`、Python 管理器 6 项测试及 py_compile 通过，Winlogbeat 8.19.0 配置通过 `test config`。需要补真实跨 offset Kafka→receipt→Raw ES→DIP/UIM ES 重放验收后才能关闭 COL-03/COL-07。
- [x] COL-08（G，P0）复用 enrollment、30 秒心跳、ETag 轮询，实现 TUBA Management Agent；报告 Beat 版本、运行状态、期望/生效配置和队列指标，离线保留有效配置。验收：管理与采集状态分别呈现，禁用能撤销来源 Kafka 写权限。
  - **2026-10-02 第一片（Agent 客户端核心，未部署）**：服务端切片（`00011_collector_management.sql`、`internal/control/collectors.go`、`internal/api/collectors.go`，enrollment/30 秒心跳/ETag 配置/禁用）此前已存在，本轮交付缺口的客户端一侧。新增 `internal/agent` 与 `cmd/tuba-agent`（enroll/run/status 三个子命令）：`Client.Enroll` 以一次性 token 换 `col_*` 身份并把凭据按 0600 存入 state 目录；`Runner` 按 30 秒节奏（可配）先做 ETag 配置轮询再发心跳，心跳携带 agent 版本、状态、期望（服务端最新）与生效（本地已接受）配置版本——`FetchConfig` 用带引号的 ETag（`"7"`，与服务端 `collectorConfig` 的比较格式一致）命中 304 时不落盘；新配置经 Agent 侧二次校验（`ValidateRemoteConfiguration`，与服务端 `containsForbiddenRemoteConfig` 同规则：拒绝 command/exec/script 等可执行指令与内联密钥，`*_env` 只许纯变量名引用）后原子写入 `config.current.json`（0600，tmp+rename），非法配置归类为 `ErrInvalidRemoteConfig`、心跳转 `state=error` 且绝不持久化；管理面不可达时保留最后生效配置继续运行并在心跳 diagnostic 说明（离线保留有效配置）；心跳或配置轮询收到 401 一律 fail-closed 为 `ErrDisabled`——标记本地 `disabled=true`、停止管理循环、不再应用新配置。测试 7 项（`go test ./internal/agent` 全过）：enroll 存凭据与 0600 权限、401 归类、新配置应用+生效版本上报+第二次轮询带 ETag 收到 304、503 离线下保留并上报最后生效版本 v3、禁用后退出且不落新配置、违禁指令配置不持久化且 state=error、配置校验 10 例正反。全量 `go build ./...`、`go vet ./...` 干净，`go test ./...` 除已知预存 flake（`internal/launcher` 的 `TestStoppedServiceSurvivesSupervisorRestart` Windows TempDir 文件锁，与本次改动无关）外全绿。**本轮不部署 248、本项不勾选**：验收的两条（管理与采集状态分别呈现——心跳内容已有但 UI/列表分列未做；禁用撤销来源 Kafka 写权限——服务端 `DisableCollector` 只禁心跳/配置，Kafka ACL/SCRAM 撤销尚未接线）均未达成；剩余切片：组件监督接线（COL-09 提供 Beat 子进程与队列指标来源后，心跳的 sources/queue_depth 才有真实值）、COL-06 过滤策略从静态文件迁入版本化下发、禁用→撤销 Kafka 写权限、Beat 版本上报与 mTLS（DESIGN-BASELINE 已定短期 token 换独立身份、后续双向 TLS）。
  - **2026-10-09 第二片（禁用联动撤销来源 Kafka 写权限，已部署 248 并完成真实闭环验收）**：验收第二条达成。实现路径取 c)（可插拔 Revoker）：新增迁移 `00015_collector_source_kafka_bindings`（collector↔source↔SCRAM principal↔source topic 绑定，`write_revoked_at` 记录 broker 侧已应用状态）；`internal/control/collector_kafka.go` 新增 `SourceWriteRevoker` 接口、绑定登记 `RegisterSourceKafkaBinding`（topic 必须是该 source 自己 context 的 `tuba.source.<ctx>.vN`，外源 topic 一律拒绝）、`EnableCollector`；`DisableCollector` 改为幂等（重复禁用 204，不再报"已禁用"），禁用提交后逐绑定撤销；`EnableCollector` 先恢复全部绑定写权限再重新启用（恢复失败则保持 disabled）。**语义选择：删 ACL 留用户**——撤销只删 WRITE ACL，SCRAM 凭据与 DESCRIBE ACL 保留，重新启用可精确逆恢复。失败不静默：撤销/恢复失败逐绑定写 `write_revoke_error`、写审计 `collector.source_write.revoke_failed`/`restore_failed`，API 返回 502 且正文带明细；审计动作 `collector.source_binding.register`/`collector.source_write.revoke`/`restore`/`collector.enable` 沿用 release/source 风格。生产 Revoker 在新包 `internal/kafkarevoke`（kafka-go DeleteACLs/CreateACLs + DescribeACLs 读回校验，双向幂等：删不存在的 ACL、建已存在的 ACL 都须收敛到验证后的终态；非 source topic 模式直接拒绝）。API 新增 `PUT /api/v1/collectors/{id}/source-binding` 与 `POST /api/v1/collectors/{id}/enable`（OpenAPI 同步，`validate_openapi.py` 44 paths 通过）。**admin 凭据走密钥文件而非清单环境变量**：`cmd/tuba-api` 从 `TUBA_KAFKA_ADMIN_PROPERTIES`（默认 `/etc/tuba/kafka-admin.properties`）读 Java-properties 格式 admin 配置——因为 Launcher 只在 supervisor 自身启动时解析服务环境，改 manifest 环境等于 manifest-wide 重启（红线），文件路径用普通 api 进程重启即可生效。248 上 `/etc/tuba` 由 0700 root 放为 0750 root:tuba（目录内所有密钥文件仍为 0600 root，已实测 tuba 仍读不到 `tuba.env`），`/etc/tuba/kafka-admin.properties` 0640 root:tuba。**过程中发现并修复两个真实缺陷**：① `CreateCollectorEnrollment` 的 `($5::text||' minutes')::interval` 在 pgx 下根本无法编码（enrollment 端点自上线起恒 503，从未被调用过）——改为 `make_interval(mins=>$5)`；② kafka-go Admin API 要求带类型的 principal（`User:<name>`），裸用户名在 DeleteACLs/DescribeACLs 过滤器里**静默匹配不到任何条目**，曾导致"撤销成功但 ACL 还在"的假阳性——revoker 统一 `User:` 前缀并留回归测试。**测试**：`internal/kafkarevoke` 13 项（stub admin 覆盖幂等、broker 错误传播、删除后仍存在/创建后不可见的校验失败、非 source topic 拒绝、typed principal 回归、properties 解析正反）；`internal/control` 集成测试（fake revoker，经 SSH 隧道对 248 真实 schema 实跑通过）：绑定登记、禁用→撤销→`write_revoked_at`、幂等再禁用零 broker 调用、启用恢复、失败回报+重试收敛、无 revoker 时响亮失败、审计事件齐全；顺带修复 `TestListSourcesIntegration` 未写 `enabled` 列违反 00014 CHECK 的预存失败。全量 `go test ./...` 28 包 0 FAIL、`go vet` 干净。**部署**（单服务路径）：包 `dist/col08-source-write-revoke/tuba-2026.10.09-col08-linux-amd64.tar.gz`（SHA-256 `789b67e1…423e43`，allowlist 26 文件审计通过，**tuba-agent 已纳入 `scripts/package_tuba.ps1` 组件清单**——allowlist 从 `$commands` 派生，自动覆盖）；api 二进制 SHA-256 `b3ba882c…a3edeb`（本地/包内/248 三处一致），旧二进制 `987ad32e…6906` 归档本机 `output/backups/248/20261009-col08/` 与 248 `/opt/tuba/bin/tuba-api.pre-col08-20261009`（21 当时从 248 不可达，未归档到 21）；迁移 00015 用 advisory lock 单迁移事务应用并登记 checksum 账本（全量脚本因 00001 历史 checksum 漂移拒绝，未强推）；`tuba-launcher restart --service api` 单服务重启，其余 10 服务 pid/restarts 未动，api ready=200。**248 真实闭环**（`scripts/verify_col08_source_write_revoke.py`，临时来源+临时 collector，四次真实来源全程未动）：20/20 PASS——注册绑定、外源 topic 绑定被拒、禁用→WRITE ACL 撤销且 DESCRIBE/SCRAM 保留、重复禁用 204 幂等、启用→WRITE 恢复、六类审计事件齐全；清理后 0 条 col08 残留 ACL/topic/SCRAM 用户，绑定行删除，测试 collector 行删除，测试 source 为 revoked（`source_contexts` 不可变，行保留）、审计事件按 append-only 保留。过程中一条临时 topic（调试重建）与 5 条残留 ACL 已清。api 清单环境变量与 `/etc/tuba/tuba.env` 均已还原（曾误改 api 的遗留 KAFKA_* 指向 adms 9192 broker 导致一次 502"No Authorizer"，属配置失误非代码缺陷，已复原并改走文件方案）。**本项仍不勾选**：验收第一条（管理与采集状态分别呈现）的 UI/列表分列未做，COL-09 组件监督接线与 COL-06 策略下发迁移仍欠。
  - **2026-10-09 第三片（UI 管理面/采集面分列呈现，已部署 248 并完成 API/资产级验收）**：验收第一条达成。
    - **数据面信号的来源判断**：现有 API 拿不到采集面信号——心跳负载（`queue_depth`、`sources[]` 每来源 events_read/sent/drop）自 00011 起已存于 `collector_agents.heartbeat` JSONB，但 `ListCollectors` 不返回它；无每来源最新事件时间的查询端点。**处置**：不新开任何端点，仅在既有 `GET /api/v1/collectors` 响应中回传已存储的心跳负载（`internal/control/collectors.go` 的 `CollectorSummary.Heartbeat`，空心跳不输出）；OpenAPI `CollectorSummary` 同步（`validate_openapi.py` 44 paths 通过）。管理面字段（state、last_heartbeat_at、online、config_version/desired_config_version）原有未动。
    - **前端**：新增 `/sources` 页面"来源与采集器"（`web/src/pages.tsx`，路由 `web/src/main.tsx`，导航 `web/src/shell.tsx`，权限 `source:manage`）。两张表均以 antd 列组**显式分列**：采集器表"管理面状态"组（管理状态 state+在线/离线、最后心跳、生效 v/期望 v 配置版本+待生效标记）与"采集面状态"组（队列积压+最早排队时间、已发送事件合计、采集健康/异常）；来源表"管理面状态"组（active/paused/revoked 生命周期、限速、release）与"采集面状态"组（数据流动指示——按 `source_id` 关联采集器心跳的 events_sent、最近错误）。无任何把两平面合并的单一状态灯；采集面无信号时明确显示"无采集信号"而不是借管理面状态冒充。zod 契约见 `web/src/api.ts`（`collectorSummarySchema`/`sourceSchema`），`web/src/api.test.ts` 新增 3 例（含从未上报心跳的 collector）。支持子路径托管：`window.TUBA_CONFIG.basePath` 作为 router basename，`/config.js` 由 Vite base 重定基。
    - **部署**（248）：api 新二进制 SHA-256 `e5c2be5a…aae9eb`，旧二进制（`b3ba882c…a3edeb`）归档 `/opt/tuba/bin/tuba-api.pre-col08ui-20261009`，`tuba-launcher restart --service api` 单服务重启，其余 10 服务未动，ready=200。**248 此前没有任何 web 静态托管**（清单无 web 服务，8443 的 `location /` 属 ADMS GUI）；本次将构建产物（`vite build --base=/tuba/`，dist 归档 SHA-256 `e1ef3709…a60eb`）部署到 `/opt/tuba/web/dist-col08ui-20261009`（`current` 软链 `dist`），并在 8443 网关 `webserver.conf` 增加 `location /tuba/`（alias + SPA fallback，`location = /tuba` 301；改前备份 `webserver.conf.bak-tuba-web-20261009`，`webserver -t` 通过）。**注意：8443 webserver 进程在本次开工前并不在运行**（首轮探测 8443 无监听），本次以其自带 `start.sh` 拉起，直接加载了含新 location 的配置；它不在 Launcher 清单内，248 重启后不会自恢复，属 ADMS 资产的既有缺口。
    - **验证**（无浏览器——浏览器受策略限制，视觉验收未做）：`https://248:8443/tuba/` 返回新 index.html（资产指向 `/tuba/assets/index-CxqAyxDm.js`）、`/tuba/config.js` 返回运行时 OIDC 配置、JS/CSS 资产 200、`/tuba/sources` SPA fallback 200、`/api/v1/me` 经网关 401（未带令牌）、ADMS `location /` 仍 200 未受影响；懒加载 chunk `pages-BjVndJKu.js`（433,234 B，200）中含"管理面状态/采集面状态/队列积压/流动中/无采集信号"等页面字符串。**API 真实数据渲染链路**：以 operator 令牌经 8443 完整走通 enrollment→enroll→heartbeat（`queue_depth:3`、来源 events_read=100/sent=98/drop=2）→`GET /api/v1/collectors` 返回该 collector 且带完整 `heartbeat` 字段（state=running、online=true、last_heartbeat_at 秒级新鲜），随后 DELETE 禁用（204）并清理测试 collector 行（审计 append-only 保留），最终 `{"items":[]}` 复原。`GET /api/v1/sources` 返回真实来源列表（state/enabled/rate_limit/source_context_id 俱全）。
    - **本项勾选**：两条验收均达成（分列呈现本片，禁用撤销写权限见第二片）。遗留不改归属：COL-09 组件监督接线、COL-06 策略下发迁移仍为各自任务。
- [x] COL-09（G/O，P1）将现有 CLI 改为受管组件监督器：单实例锁、独立 registry/data、进程身份、优雅停止、崩溃退避、签名组件升级、状态格式兼容检查和回滚。验收：跨 OS 相同命令，不注册 systemd/Windows Service；主机重启恢复方式明确。
  - **2026-10-09 第一片（受管监督器核心：签名升级 + 状态格式兼容检查 + 守卫回滚 + 单实例锁；未部署 248，不勾选）**：
    - **新包 `internal/component`**。**签名清单**：`manifest.json`（schema_version=1、component、version、os/architecture、`state_format{version,min_readable}`、files 相对路径→SHA-256、ed25519 签名）——签名覆盖移除签名字段后的 canonical JSON（结构体字段序 + map 排序键确定性编码），keyring 为 key_id→ed25519 公钥 JSON 文件，未知 key_id/坏签名/错 OS-arch/路径逃逸（`..`、绝对路径、反斜杠）/非法 digest 全部拒绝。**升级流程 `Upgrader.Apply`**：验签→逐文件哈希校验且包内不得有清单外文件（防夹带）→实例状态格式兼容检查→磁盘空间检查→`components/<name>/.staging-*` 暂存并对落盘副本**二次验哈希**→rename 为不可变版本目录→原子切换 `current.json` 指针（`previous.json` 保留回滚目标）→写 `upgrade-pending.json` 升级日志；未 confirm/rollback 前拒绝第二次 Apply（防丢回滚目标）。指针用 JSON 文件而非 symlink/junction，**跨 OS 语义逐字节一致**（Windows 目录链接需特权）。**状态格式门禁**（双向 fail-closed）：已戳记格式 F 须满足 `min_readable ≤ F ≤ version`——F 更新则新版二进制读不了、F 过旧则需显式迁移工具，均拒绝；无戳记的非空数据目录拒绝猜测；`StampStateFormat` 只进不退。**守卫回滚 `Rollback`**：回滚前用**回滚目标版本已安装清单（重新验签）**的兼容范围核对每个实例当前数据格式——数据已被新版本迁移到旧版读不了的格式时**拒绝回滚**（即设计基线"失败不能盲目回滚可执行文件后复用不兼容数据目录"），失败版本目录保留取证、只动指针。**Confirm**：健康观察后提交，并把实例数据目录戳记为新版本的格式版本（组件可能已迁移数据）。**单实例锁 `InstanceLock`**：`<root>/agent.lock` 绑定 pid+进程身份，**复用 `internal/launcher` 的 boot_id+启动时刻/进程创建时间语义**（本轮在 launcher 加了两个导出包装 `ProcessIdentity`/`ProcessInstanceAlive`，行为零变化）——活持锁者 fail-closed 拒绝，陈旧（死 pid/上一 boot/身份不符）或不可解析的锁可替换，Release 只删自己那代锁。
    - **监督循环不重写**：崩溃退避、优雅停止、runner_identity、per-service desired state 全部沿用 `internal/launcher` 现有机制（受管组件以 launcher manifest 服务运行，版本切换后走 `restart --service` 粒度），本包只管磁盘版本布局与升级/回滚决策，不产生第二套语义。
    - **CLI `cmd/tuba-component`**：`verify`/`apply`/`confirm`/`rollback`/`status`/`stamp-format` 六个子命令，Windows/Linux 相同命令行，不注册 systemd/Windows Service；已加入 `scripts/package_tuba.ps1` 组件清单（allowlist 自动覆盖）。
    - **测试**（`go test ./internal/component` 7 个测试函数全过，含正反与篡改用例）：签名/哈希/未知 key/错 OS/路径逃逸/夹带文件拒绝；首装→pending 日志→二次 Apply 拒绝→Confirm 戳记状态格式；升级→兼容回滚（版本目录保留取证、previous 指针互换）；**数据迁移到 format 3 后回滚到只写 format 2 的版本被拒绝且 current 保持不动**；数据过新/过旧/无戳记非空目录三个门禁；戳记回退拒绝；版本目录不可变重装拒绝；组件名错配拒绝；锁的互斥/释放/陈旧替换/腐坏替换。`go build ./...`、`go vet ./...` 干净（linux/amd64 交叉构建同过）；全量 `go test ./...` 29 包 0 FAIL（`internal/launcher` 的 `TestStoppedServiceSurvivesSupervisorRestart` Windows TempDir 文件锁预存 flake 本轮未再复现，单测 5/5 通过）。
    - **与验收标准的差距（后续切片）**：~~① 组件 current 指针与 launcher manifest 的联动编配~~（已由第二片解决，见下）；~~② 健康观察窗口的自动回滚编配~~（已由第二片解决）；~~③ 包下载/限速/灰度与组件发布仓库~~（已由第三片解决，见下）；~~④ 与 COL-08 agent 心跳的组件版本/重启计数上报接线~~（已由第四片解决，见下）；⑤ 真实 Beat 包（`tuba-managed-collectors-v1.zip` 需加签名清单）端到端验收与 248 部署。主机重启恢复方式沿用既有结论（`@reboot tuba-boot`，见 O04），本切片不改变它。
  - **2026-10-09 第二片（升级编配器：生命周期联动 + 健康观察窗口 + 自动回滚；未部署 248，不勾选）**：弥合第一片差距①②。
    - **`internal/component/orchestrator.go` 新增 `Orchestrator`**：把 verify/apply/confirm/rollback 原语编成设计基线的升级序列——停组件→`Apply`（验签/哈希/状态格式/磁盘门禁全部在位）→启动→健康观察→`Confirm`；观察期内不达标→自动 `Rollback`（状态格式守卫在位）→重启旧版→**对回滚结果再做一次完整观察**→健康则以 `RolledBackError` 收尾（组件跑旧版、phase=idle、rolled_back=true），回滚后仍不健康则进入**终态 failed 并停掉组件**（fail-closed，不无限震荡，只走一轮 stop/start + 一轮回滚 stop/start + 终态 stop）。回滚被状态格式守卫拒绝（数据已迁移）同样落 failed。pending 日志未解决时拒绝新升级（`ErrOrchestrationBusy`）。
    - **健康判据**：进程存活（`ProcessManager.Alive`）+ readiness 端点（`HealthChecker.Ready`）双条件；观察窗口默认 5 分钟（设计基线），启动宽限默认 1 分钟（宽限内 not-ready 容忍，进程死亡任何时刻立即失败），轮询默认 2 秒，均可配。**进程管理复用 launcher**：生产实现 `LauncherProcessManager`（`adapters.go`）走 `launcher.StopServices/StartServices` 单服务粒度；launcher 新增导出 `QueryService`/`ServiceAlive`（`identity_export.go`），**supervisor 不存活时 fail-closed 报错**（与 status 的陈旧状态规则同源）；本包不自建任何监督循环/退避/信号路径。`HTTPHealthChecker` 按既有 `/health/ready` 约定要求 200。
    - **状态机可查询**：每次迁移原子落盘 `<root>/components/<name>/orchestration.json`（phase: idle/applying/observing/confirming/rolling-back/recovering/failed、from/to 版本、rolled_back、error、时间戳），`Upgrader.Status` 输出新增 `orchestration` 字段，`tuba-component status` 直接呈现。
    - **CLI**：`tuba-component upgrade --root --keyring --package --manifest --service --health-url [--observe 5m --grace 1m --poll 2s --instance ...]`——同一条命令 Windows/Linux 一致；回滚恢复时打印明确结果并以非零退出（升级未生效）。
    - **测试**（`go test ./internal/component -count=10` 全过）：编配成功路径（调用序列 stop,start、日志清除、phase 回 idle）；观察期持续不健康→自动回滚→旧版恢复（`RolledBackError`、current 回退、调用序列 stop,start,stop,start）；**回滚后仍不健康→failed 终态+组件停止+恰好两轮调用无震荡**；观察期内进程崩溃→自动回滚（崩溃注入由健康探针在首个观察探针处确定性触发，避免相位轮询竞态——此前用 sleep/轮询编排在压测下不稳定，改为确定性挂钩后 -count=10 稳定）；pending 日志拒绝并发升级；`HTTPHealthChecker` 200/503 正反（httptest）；`Status` 暴露编配状态。`go build`/`go vet` 干净，linux/amd64 交叉构建同过；除 `internal/launcher` 预存 flake（`TestStoppedServiceSurvivesSupervisorRestart` Windows TempDir 文件锁，**在未改动的 stash 树上同样复现**，与本次改动无关）外全量 28 包 0 FAIL。
    - **本切片仍不覆盖**（保持第三片及以后）：包下载/限速/灰度与发布仓库、agent 心跳接线、真实 Beat 包与 248 部署验收。
  - **2026-10-09 第三片（发布仓库下载/限速 + 签名打包接入制品链 + 灰度滚动；未部署 248，不勾选）**：弥合第一片差距③。
    - **签名密钥纪律（仓库此前无任何签名设施，本片定最小流程）**：ed25519 签名私钥由发布操作员**离线生成并保管**——`tuba-component genkey --key-file --key-id [--keyring-out]` 生成密钥对，私钥 0600 落盘、**拒绝覆盖既有密钥文件**（轮换是显式动作）、Unix 下权限非 0600 拒绝读取；公钥经 `--keyring-out` 并入 keyring（同 key_id 不同值拒绝静默变更）。`.gitignore` 新增 `*.ed25519.key`、`component-signing-key*`，私钥永不入库。仓库内无私钥、文档无密钥。
    - **签名接入 COL-02 制品链**：`internal/component/sign.go` 的 `BuildManifest`/`SignPackage` 给组件目录生成签名 `manifest.json`（递归哈希全部常规文件、拒绝原位重签）；`scripts/package_managed_collectors.ps1` 新增可选 `-SignKeyFile`/`-SignKeyID`/`-StateFormatVersion`/`-StateFormatMin`——打包时对每个组件目录调用 `tuba-component sign`，产物 bundle 的每个组件目录即携带受管监督器可消费的签名清单；不签名时行为不变（PowerShell 语法解析校验通过；全量重打包需 Elastic 制品网络下载，本轮未重跑）。
    - **下载器 `internal/component/download.go`**：发布仓库为普通 HTTP(S) 文件树 `<base>/<component>/<version>/{manifest.json,<files...>}`（248 API 只登记 release manifest、无资产下载端点，文件树是不新造服务端的最小形态）。`Downloader.Fetch` 顺序：先拉 manifest 并**在任何载荷字节下载前完成验签与结构校验**（含组件名/版本与请求一致），再逐文件流式下载+SHA-256 边下边算、单文件 2 GiB 上限、可选限速（`--rate-limit-kbps`，时间预算节流）；全程写入同文件系统的 `.download-*` 暂存目录，任何失败**整目录清除**、只有全部验证通过才 rename 到位——部分下载不留半成品，目标已存在即拒绝覆盖。
    - **灰度 `internal/component/rollout.go`**：`RunRollout` 按批次（可配 batchSize）逐目标跑完整编配升级（每目标独立 root/PM/健康检查，天然支持多机）；任一批次内首个失败即**中止后续批次**，并把此前已 confirm 的目标经 `Orchestrator.Revert`（停→守卫回滚→重启→完整健康观察）逐一撤回旧版；失败目标本身已由编配器自动回滚。报告区分 confirmed/reverted/auto-rolled-back/failed。撤回失败不中断其余撤回（目标是全舰队回旧版），错误聚合上报。
    - **CLI**：新增 `genkey`/`sign`/`fetch` 子命令，跨 OS 一致；`--root` 改为仅监督类命令必填。
    - **测试**（`go test ./internal/component -count=3` 全过）：签名 round-trip、密钥文件 0600/拒绝覆盖/Unix 权限放宽拒绝；下载 happy path（验签+逐文件哈希+目标可 LoadManifest+拒绝覆盖既有目录）、错误 key 验签拒绝且父目录零残留、签名后篡改载荷被哈希门禁拦截且零残留、**截断 body 失败且零暂存残留**、manifest 名实不符拒绝、限速实测（256 KiB @ 128 KiB/s 预算 2s、实测 ≥1.5s 放行阈值）；灰度三例：全健康 4 目标两批全 confirm、批次 2 首目标失败→批次 1 两目标撤回旧版且状态机 idle/rolled_back、批次 1 失败→后续目标**零调用**零改动。`go build`/`go vet` 干净，linux/amd64 交叉构建同过；除 `internal/launcher` 预存 flake（未改动 stash 树同样复现）外全量 28 包 0 FAIL。
    - **本切片仍不覆盖**：COL-08 agent 心跳接线（下载/编配/灰度原语均已就位）、真实 Beat 包签名端到端验收与 248 部署。COL-09 仍不勾选。
  - **2026-10-09 第四片（Agent 心跳接线：心跳上报组件真实状态 + 升级指令驱动编配；未部署 248，不勾选）**：弥合第一片差距④。
    - **心跳合同扩展（服务端→OpenAPI→Web 三层同步）**：`control.CollectorHeartbeat` 新增 `components[]`（component、version、phase、state、restarts、last_error），验证边界齐全（≤64 组件、名字 1..64、phase 枚举限定为编配器七相位、state 复用 running/paused/error、restarts≥0、重复组件拒绝——校验抽到 `validateCollectorHeartbeat` 以便无库测试）；心跳 JSONB 落库与 `ListCollectors` 读回自动透传；OpenAPI `CollectorHeartbeat` 同步（`validate_openapi.py` 44 paths 通过）；Web `collectorComponentStatusSchema`/`collectorHeartbeatSchema` 同步（vitest 7/7 过）。
    - **Agent 侧接线 `internal/agent/supervision.go`**：`Supervisor` 登记本地受管组件拓扑（`SupervisedComponent`：root/keyring/repo/launcher manifest+service/health URL——**本地拓扑决定如何停起和健康检查，管理面只能说跑哪个版本**）。远程配置可含 `components: {<name>: {desired_version}}` 指令段（结构天然通过 `ValidateRemoteConfiguration`：无可执行键、无密钥形键）；新配置**持久化接受后**才触发指令；未知组件名/非法版本 fail-closed 拒绝并上报 error diagnostic，绝不执行。升级异步执行：先按 keyring 验签复用本地下载缓存，否则 `Downloader.Fetch` 先验签后下载到 `<root>/downloads/<component>-<version>`，再跑完整 `Orchestrator.Upgrade`（含状态格式门禁、观察窗口、自动回滚）；同组件并发升级去重。心跳经 `Supervisor.ComponentStatus()` 携带真实值：版本取自 `current.json` 指针、相位取自 `orchestration.json`、state/restarts 取自 launcher 查询（`QueryService`/`ServiceAlive`，supervisor 不存活则降级 error 而非粉饰）。
    - **禁用语义（按 COL-08/COLLECTOR-CONTROL-PLANE §2 确认）**：禁用只停管理循环（不再接受新配置/新升级指令），**组件进程继续运行**——采集写权限已在 broker 侧被 COL-08 第二片撤销，存量数据按冻结上下文处理；进行中的升级不被打断（编配是本地且带健康守卫，会自行 confirm 或回滚）。联合测试 `TestAgentDisabledKeepsComponentsRunning` 断言禁用后组件零 stop 调用。
    - **测试**：`internal/agent/supervision_test.go` 四例（fake 管理面 + fake PM/health + httptest 发布仓库，真实签名包）：① 基线心跳携带 v1 真实组件状态→下发 v2 指令→编配完成→心跳上报 8.19.1+phase idle+running，缓存包可重新验签；② 新版本不健康→自动回滚→心跳上报回到 8.19.0、state=error、last_error 带 rollback 原因；③ 未知组件指令被拒（state=error+diagnostic、filebeat 版本不动、PM 零调用、配置本身仍按版本接受）；④ 禁用→管理循环停、组件继续运行。`internal/control` 新增组件心跳校验 6 例。`go build`/`go vet` 干净；`go test ./internal/agent -count=3`、agent/control/component 三包及全量（除 `internal/launcher` 预存 flake，未改动 stash 树同样复现）28 包 0 FAIL。
    - **剩余差距**：仅 ⑤ 真实 Beat 包签名端到端验收与 248 部署（含真实 Winlogbeat 组件以 launcher 服务形态接入受管监督）。
  - **2026-10-10 第五片（真实 Beat 包签名端到端验收 + 248 部署；本项勾选）**：
    - **新增 `tuba-component run`**：launcher 清单 command 路径是静态的，版本切换需要稳定入口——`run --root --component --bin <rel> -- [args]` 解析 `current.json` 指针并 exec 真实二进制（Unix `syscall.Exec` 保持 PID 使 launcher 追踪精确；Windows 子进程透传 stdio/退出码）；`-c` 相对配置路径解析到当前版本目录（配置随签名包走）。测试覆盖解析、路径逃逸拒绝、无 active 版本 fail-closed。
    - **签名密钥**：`tuba-component genkey` 生成 key_id=`tuba-components-2026`；私钥存本机 `.runtime/col09/`（0600，gitignored，不入库不进报告），公钥入 `deploy/components/component-keyring.json`（可入库，仅公钥）。
    - **真实签名包**：对 COL-02 bundle 的三个真实组件目录签名并验签通过——filebeat 8.19.0 linux-amd64（1456 文件）、filebeat 8.19.0 windows-amd64（1457 文件）、winlogbeat 8.19.0 windows-amd64（141 文件）。
    - **Windows 本机验收（真实 Winlogbeat）**：verify→apply→confirm→`run` 经 current 指针真实执行 `winlogbeat.exe version`（输出 8.19.0/libbeat 8.19.0）→本地 HTTP 仓库 fetch 8.19.1 伪版本（限速参数生效）→apply→confirm→rollback 回 8.19.0，status 全程正确。
    - **248 部署**：`/opt/tuba/bin/tuba-component`（linux/amd64，SHA-256 `aab520eb…b1cb95`，本地/248 一致，sha256 sidecar 已写并 `sha256sum -c` 通过；**首装无旧件**）；`/etc/tuba/component-keyring.json` 0640 root:tuba（仅公钥，tuba 可读实测）。生产两份清单全程未动。
    - **248 端到端验收（隔离验收链路，不与现役链路冲突）**：验收根 `/opt/tuba/col09-acceptance/`，独立 launcher 清单/独立 state/log/独立端口（18941 仓库、18942 filebeat http），filebeat 输入只读验收目录、输出只写验收目录（file output），**不接触任何生产 topic/消费组**。实测序列：fetch 8.19.0（先验签后下载，1456 文件）→apply→confirm（实例状态格式戳记）→launcher 拉起真实 filebeat（`run` exec，pid 直挂）、`/stats` 200、**marker 日志经 filebeat 真实落进输出 ndjson**；升级到 8.19.1 首轮因打包丢可执行位真实失败→**自动回滚 8.19.0 并恢复健康**（意外收获的真实回滚证据）；补 chmod 后以 8.19.1a 重签重试→**编配升级成功**（stop→apply→start→15s 观察→confirm，current=8.19.1a，phase=idle）；升级 8.19.2（坏 YAML 配置，签名/哈希合法）→真实 filebeat 启动即退→存活检查失败→**自动回滚 8.19.1a 并恢复**，exit 1、status rolled_back=true；错误 keyring fetch 被拒且零残留。验收后环境全部清理（launcher 停、仓库进程杀、目录删、端口释放），生产 11+5 服务 running 逐行核验未动。**口径说明**：验收链路是真实组件真实运行+真实编配，但不是现役 Zeek/Windows 采集链路的受管化切换——现役链路切换属运维决策（影响评估先行），不在本项验收标准内。
    - **验收判定（三条全部达成，本项勾选）**：① 跨 OS 相同命令——tuba-component 同一组子命令（verify/apply/confirm/rollback/upgrade/status/stamp-format/genkey/sign/fetch/run）在 Windows 与 Linux 实测一致；② 不注册 systemd/Windows Service——全程 launcher+文件指针，无注册；③ 主机重启恢复方式明确——沿用 `@reboot tuba-boot`（O04 已两次真实重启验收），受管组件作为 launcher 服务随清单拉起。条目全清单（单实例锁/独立 registry+data/进程身份/优雅停止/崩溃退避/签名升级/状态格式兼容检查/回滚）均有代码+测试+本轮真实验收证据。
    - **已知限制（不阻塞勾选，记为后续改进）**：下载落盘的文件权限固定 0644（manifest 只携带内容哈希不带 mode），Linux 组件二进制需在 fetch 后补 chmod +x（本轮验收即如此）；后续可在 manifest 增加 per-file mode 位。COL-08 心跳的组件字段已接线上报，但 248 现役采集链路尚未跑 tuba-agent（COL-08 部署决策另行）。
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
  - **2026-10-01 可查询性验收（D1 画勾证据第 4 条，只读查询，未改任何配置）**。"暂时重试 vs 永久 DLQ"的运维查询路径已逐一实测：
    1. **组件计数器（Prometheus /metrics，进程生命周期内累计）**：zeek source-adapter 在 `127.0.0.1:19185/metrics`（端口取自 `/etc/tuba/tuba-services.json` 的 `SOURCE_ADAPTER_METRICS_LISTEN`），实测当前值：`events_fetched_total 21705 = events_accepted_total 21705 = offsets_committed_total 21705`（恒等 = 无积压无跳过）、`kafka_fetch_retries_total 1`、`stall_events_total 1`、`topic_missing_events_total 2`、`consumer_replacements_total 1`（即 10-01 topic 重建自愈复验的留痕）。暂时重试对应 `delivery_retries_total`、永久 DLQ 对应 `dlq_written_total`/`events_rejected_total`、DLQ 写失败（此时不提交 offset）对应 `dlq_write_failures_total`（`internal/sourceadapter/adapter.go:174-195`）。**口径注意**：计数器是首次非零时才注册的（`internal/telemetry/metrics.go:41`），当前这三个计数器自进程启动以来为零，因此 `/metrics` 与 Prometheus（19090 实查 0 series）里**不出现**——"查不到"等于"自本次启动以来没发生过"，而不是"没接监控"。历史发生过的重试（COL-07b 3/5 的 `delivery_retries +13`）在进程重启后不可回溯，长期趋势只能靠 Prometheus 在事件发生时的抓取。
    2. **日志**：Launcher 私有日志 `/var/log/tuba/zeek-source-adapter.log`，实测含停滞/自愈记录（如 `2026/10/01 10:50:26 ... consumption stalled: topic does not exist in broker metadata`、`10:52:27 ... topic is back; consumer replaced ... resuming at FirstOffset`、`10:52:28 ... consumption recovered`），即"暂时不可用"状态在日志可见。
    3. **消费组位点**：`kafka-consumer-groups.sh --describe` 的 lag 显示"还在重试中"（位点不前进）；永久失败则在提交 offset 后前进，记录同时出现在 DLQ topic（见 I06 验收记录）。
    4. **DLQ 不可用时不提交输入**：代码路径为 DLQ 写失败即返回错误、不提交 offset（`adapter.go:178-181`）；COL-07b 1/5 flood-stage 实验实测该行为（三 DLQ topic end offset 全程不动）。
    **结论**：N07 的"区分"语义在 248 上有可查询状态（计数器+日志+lag+DLQ topic 四路），但有两处口径限制已写明：零值计数器不可见、重启后历史不可回溯。N07 本项其余子句（过期数据不自动重建已删除索引）未在本轮单独验证，本项仍不勾选。
- [ ] N08（G/O）在隔离 namespace 对照旧 ES Pipeline 输出，记录差异并修正规则；生产来源切换时只激活一条路径。

阶段出口：真实 Windows/Zeek 原始样例经过同一可信链路进入八领域，标准消息可供索引与分析共同消费。

## 阶段 5：任务与一致性基础（P0；依赖 C06、C09、阶段 2）

- [ ] T01（G）创建 control-worker：任务状态机、attempt、租约、fencing、重试、取消、心跳与 SKIP LOCKED 领取。
  - **2026-10-12 进展（代码与单测层完成，集成测试待真实 PG 执行，不勾选）**：盘点确认框架骨架此前已存在（`internal/controlworker/jobs.go` 的 SKIP LOCKED 领取、租约 fencing、心跳、过期回收、queued 取消、有界退避；表结构复用 `00007` 的 `processing_jobs`/`processing_job_attempts` 与 `00008` 的重试字段，无新迁移）。本轮增量：新增 `internal/controlworker/state.go` 把任务状态机形式化为唯一权威——`JobState`/`AttemptState` 枚举、`CanTransition` 合法迁移表（queued→{running,cancelled}、running→{queued,succeeded,failed,cancelled}、三个终态无出边）、`RetryBackoff`（2^min(attempt,8) 秒、上限 300s、实际封顶 256s，与 SQL 表达式同步）、`FinishTransition` 完成决策函数（取消优先于一切；worker 停止即立即重排队且 attempt 记 lease_expired、不耗重试预算；handler 错误在预算内重排队、耗尽转 failed）。`finishJob` 重构为：事务内 `SELECT ... FOR UPDATE` 带 fencing 守卫重读取消标志 → `FinishTransition` 计算结果 → 非法迁移 fail-closed 拒绝后才 UPDATE；旧租约持有者的写入因 fencing 不匹配零行受影响而失败。心跳续约抽出为可测的 `renewJobLease`。测试：`state_test.go` 7 例全过（完整迁移矩阵、终态无出边、退避有界且单调、取消/成功/重试/耗尽/停机各语义）；新增 `jobs_integration_test.go` 7 例（并发领取同一任务仅一个 worker 成功且只有一条 running attempt、fencing 拒绝旧租约 finish/heartbeat 且任务状态不被污染、心跳续约且他人不可代续、queued/running 两种取消语义、重试退避持久化且有界+耗尽转 failed、停机重排队不耗预算、过期租约回收按预算分流 queued/failed）——沿用 `TUBA_TEST_DATABASE_URL` 门控，用专用一次性 organization 夹具，跑完清理，不触碰租户数据。**2026-10-12 集成测试已对 248 真实库实跑通过（经 SSH 隧道）**：初跑 2/7 失败，根因是测试假设"回收/重排队后可立即领取"，与生产语义"重排队带有界退避（next_attempt_at）"冲突——属测试口径缺陷而非实现缺陷，已在断言退避有界后由测试拨正 `next_attempt_at` 再领取；修正后 7/7 全过，`processing_jobs` 无测试残留。`go build ./...`、`go vet ./...`、全量 `go test ./...` 0 FAIL。勾选缺口：仅剩入口 `cmd/tuba-control-worker` 无业务 handler 注册（属后续 T02/分析任务接线范围）。
- [x] T02（G/P/D）实现 inbox＋状态＋checkpoint＋outbox 单事务协议，恢复以 PG checkpoint 为权威；处理 rebalance/旧租约写入。
  - **2026-10-12 完成并勾选（协议层，对 248 真实库集成验收通过）**。表结构无需新迁移——`00007` 的 `processor_inbox`/`processor_checkpoints`/`processor_outbox` 与 `00008` 的租约字段已齐备，248 台账停在 00015、未变动。实现为 `internal/controlworker/transaction.go` 的 `ProcessInboxMessage`：一条输入消息的 inbox 去重记录、handler 业务状态写入（经传入的 `pgx.Tx` 与提交同生共死）、checkpoint 推进、outbox 行（按 producer+aggregate_key 事务内分配 sequence，sha256 payload_hash）全部在**同一 PG 事务**内提交；任何一步失败整事务回滚、不留痕迹，恢复后重放即可。**两层 fencing 同事务生效**：主防线是 job 行 `SELECT ... FOR UPDATE` 且须 state=running+lease_owner+fencing_token 匹配（旧租约持有者零行命中、`ErrStaleFencing` fail-closed）；纵深防线是 checkpoint 只接受 ≥ 已存 fencing token 的写入，回退 offset 一律 `ErrCheckpointRegression` 拒绝，重提交当前位置为安全 no-op。**重复/重放语义**：inbox 按 (consumer_group, message_id) 去重——崩溃后同 offset 或跨 offset 重投都判 duplicate，handler 不重跑、outbox 不重发，仅推进 checkpoint（相当于持久 offset commit）；同一 transport position 被不同 message_id 认领时由 `UNIQUE(consumer_group,topic,partition,message_offset)` fail-closed 拒绝。恢复权威为 `LoadCheckpoints` 返回的 PG checkpoint，worker 重启/换主后从 next_offset 续跑。outbox 实际 Kafka 投递与聚合键顺序仍属既有 `outbox.go` publisher（T03 范围），本协议保证其行恰好产生一次。
  - **测试**：单测 `transaction_test.go`（输入/业务输出/checkpoint 校验、payload hash、nil work 拒绝）；集成 `transaction_integration_test.go` 6 例，全部经 SSH 隧道对 248 真实库实跑通过，并自建一次性 organization＋专用 consumer group 夹具、跑完零残留（已实测 inbox/checkpoint/outbox/jobs/organizations 五处计数均为 0）：① 同事务提交后 inbox/checkpoint(fencing 匹配 job token)/两条 outbox(sequence 1,2、hash 正确） 全部落库；② 崩溃在提交前——inbox/checkpoint/outbox 零痕迹，恢复后重跑恰好一份 outbox；③ 提交后重投（同 offset 与跨 offset）均判 duplicate、outbox 不增、checkpoint 推进；④ rebalance 后旧持有者写入被 fencing 拒绝且零痕迹、新持有者正常续跑，checkpoint 被更高 token 占用时写亦被拒；⑤ PG checkpoint 权威恢复、旧位置重放与回退提案被拒且回滚、当前位置重提交 no-op；⑥ 业务状态写入与协议同提交/同回滚。过程中修正两处测试口径缺陷（旧位置重放先撞 inbox 位置唯一约束——属正确 fail-closed 行为而非实现缺陷；edge no-op 用例改用新位置）。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 31 包 0 FAIL。**范围说明**：协议层无生产消费者接线（属 F02/分析任务范围），V03 的故障点验收仍为独立条目。
- [x] T03（G）实现有界 outbox publisher、聚合键顺序、投递重试和超时告警；无业务状态跨进程共享文件。
  - **2026-10-12 完成并勾选（对 248 真实库集成验收通过）**。在既有 `internal/controlworker/outbox.go` 骨架（有界领取、SKIP LOCKED、租约 fencing、退避释放）上增量补齐，表结构**无新迁移**（248 台账仍为 00015）：
    - **有界领取**：`BatchSize` LIMIT + `FOR UPDATE SKIP LOCKED` 不变；新增 `MaxAttempts`（默认 8）——claim 过滤 `attempts < MaxAttempts`，耗尽行不再被任何 publisher 领取：保持 unsent、fail-closed 地**继续阻塞其聚合键**（NOT EXISTS 守卫对 unsent 前置行生效），经 `outbox_dead_records` 仪表与错误日志暴露，运维重排队口径为 `UPDATE processor_outbox SET attempts = 0 WHERE id = ...`。
    - **聚合键顺序**：批内按 (producer, aggregate_key, sequence) 排序后按键分组（`groupByAggregateKey`），同键严格顺序投递（组内任一记录失败/失租约即中止该组本轮剩余记录——顺带修复了旧代码 `continue` 允许同批跳过失败前置记录的顺序漏洞），不同键并发、并发度由 `MaxConcurrentKeys`（默认 4）信号量封顶。
    - **有界重试**：投递失败释放租约并写 `next_attempt_at = now() + LEAST(300, 2^min(attempts,8))s`、`last_error` 截断 1024；attempts 在领取时递增，达到上限转入上述 dead 语义（`outbox_dead_total` 计数、错误日志指明阻塞后果）。
    - **超时/卡死告警**：`PublishTimeout`（默认 10s）以 context 包住 `WriteMessages`；每轮轮询执行健康扫描（无共享文件、全部以 PG 为权威），产出可查询状态——仪表 `outbox_pending_records`/`outbox_dead_records`/`outbox_oldest_unsent_seconds`，卡死信号 `outbox_stall_events_total`（对 unsent、仍可重试、`created_at` 超过 `StuckThreshold`（默认 5min）的行每行每进程告警一次 + Warn 日志）。`internal/telemetry.Registry` 新增 `Set`（gauge）与 `Value`（测试读回），计数器语义不变。
    - **崩溃不丢不重**：broker ack 后、mark sent 前崩溃 → 租约过期后由任一 publisher 重领重投（至少一次），行携带 `outbox-id`/`producer`/`aggregate-key`/`aggregate-sequence` 头供下游去重（与 T02 inbox 去重组合为既定"至少一次+去重"口径）；mark sent 失败计 `outbox_mark_sent_failures_total` 并保持行可重投。publisher 全部状态只在 PG，无任何跨进程共享文件。
    - **衔接 T02**：只有已提交事务的 outbox 行在表中可见，publisher 只读已提交行；投递成功标记 `sent_at`、清理 lease 列（00008 的 lease_pair CHECK 约束保持满足）。
    - **测试**：单测 `outbox_test.go` 4 例（配置默认值/显式保留、按键分组保序、非法 JSON 不触 writer、投递头完整含四元组、metrics nil 安全）；集成 `outbox_integration_test.go` 5 例经 SSH 隧道对 248 真实库实跑全过（并发用例 `-count=3` 复跑稳定），专用 `outbox_it_*` producer 前缀夹具、跑完 DELETE 清理，实测 248 `processor_outbox`/`processing_jobs` 零测试残留：① 同键顺序——key blocked 的队头持续失败时 seq 2/3 零投递且整键保持 unsent，key ok 按 1→4 顺序投递；② 崩溃续投——ack 后 mark 前崩溃、租约过期、新 publisher 重投恰好 2 次投递后 sent、租约清空；③ 有界重试——MaxAttempts=2 耗尽后 attempts 停在 2、退避 ≤300s、last_error 留痕、再无投递（`outbox_dead_total`≥1、可重试失败恰 1 次）；④ 超时告警——卡死行触发 `outbox_stall_events_total`≥1 且 `outbox_pending_records`/`outbox_oldest_unsent_seconds` 仪表有值；⑤ 并发 publisher——双 publisher 20 行 5 键，每 id 恰好投递一次（SKIP LOCKED 无重复领取）、每键按序、两个 publisher 均参与。过程中修一处测试口径缺陷（双 writer 记录各自追加、合并后按追加序判定同键顺序导致假乱序，改为按投递时刻合并排序；非实现缺陷）。**Kafka 投递口径**：writer 抽象 `MessageWriter` 不变、入口 `cmd/tuba-control-worker` 仍接真实 `kafka.Writer`（RequireAll、Hash balancer），测试使用记录型 fake writer，不触 248 生产 topic——真实 broker 投递路径与既有部署一致，未自建测试 topic。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 0 FAIL。
- [x] T04（G）受权限控制、可审计的 release 发布闭环。代码/API/合同和手册已交付；248 的 00012/00013 迁移、首位 publisher 引导和新版 API 部署已完成。release 资产不可原位覆盖；source 必须显式绑定 staged/active release。2026-09-30 复核闭环证据：以既有开发操作员 `tuba-operator-20260929`（subject `26a64c67-b05a-4d52-b08f-201a8657ec03`，即首位 bootstrap publisher）运行 `scripts/publish_release.py`，本地 bundle 校验通过，`windows-security-1.0.0` 已登记并完成 draft→validated→staged→active（各转换发生于 2026-09-29 19:38:07–08 +08:00，幂等重放确认无重复创建）。读回证据：`GET /api/v1/releases/windows-security-1.0.0` state=active、sha256=`0195fd13…35a74`，服务端 manifest canonical digest 与仓库 `releases/windows-security-1.0.0/` 本地计算一致；`/audit` 返回 4 条事件（release.create/validate/stage/activate），actor 均为 `26a64c67-…-201a8657ec03`，各带独立 request ID。未新建临时身份，无需清理。
- [x] T05（G/D）实现 inbox/outbox/state retention、任务临时文件清理、活跃租约/证据保留保护。
  - **2026-10-12 完成并勾选（对 248 真实库集成验收通过）**。表结构**无新迁移**（248 台账仍为 00015）；新增 `internal/controlworker/retention.go` 的 `RetentionCleaner`，作为 control-worker 的第三个周期性内置任务接入 `cmd/tuba-control-worker`（与 outbox publisher、job worker 同进程，沿用既有 Run/PollInterval 惯例），保留期由 `CONTROL_WORKER_INBOX_RETENTION`（默认 48h，与 `scripts/prune_ingest_receipts.sh` 的窗口同源——inbox 去重的边界是可能重投的 Kafka 保留期，24h 的两倍）、`CONTROL_WORKER_OUTBOX_RETENTION`（默认 48h）、`CONTROL_WORKER_SUCCEEDED_JOB_RETENTION`（默认 7 天）、`CONTROL_WORKER_FAILED_JOB_RETENTION`（默认 30 天）与 `CONTROL_WORKER_RETENTION_POLL_INTERVAL`（默认 1h）配置，非法值回退默认并告警。
  - **保护语义（fail-closed）**：① outbox 只删 `sent_at IS NOT NULL AND sent_at < cutoff AND lease_owner IS NULL` 的行——未投递、重试中、耗尽未重排队（T03 dead 语义阻塞聚合键）及任何带租约的行一律保留；② 任务 state 只删终态且 `finished_at` 超期的行，succeeded 用 7 天窗、failed/cancelled 用 30 天证据窗（`last_error`/attempts 即失败证据），queued/running、无 `finished_at`、带 `lease_owner`（活跃租约）、未知状态的行一律保留；attempt 行随任务 `ON DELETE CASCADE`；③ 删除条件全部编码在常量 SQL 中，cutoff 一次解析后固定（同 prune 脚本惯例，防止移动 cutoff 导致循环不终止），ctid 分批（默认 1 万/批）+ 每表批数上限 1000 防失控；任何 SQL 错误中止该表本轮清理并计 `retention_sweep_failures_total`，不继续猜。
  - **任务临时文件（登记+清理最小形态）**：仓库此前没有任务临时文件设施。登记：`RegisterJobTempDir(root, jobID)`/`JobTempDir`——按 job id 建 `<root>/<job-id>/`（0700），job id 必须是 canonical UUID 否则 fail-closed 拒绝（杜绝路径逃逸）。清理：清理器在删除任务行时连带删其 temp 目录，并扫描 `TempRoot`（`CONTROL_WORKER_JOB_TEMP_ROOT`，空则停用文件清理、PG 清理照常）下的 UUID 目录——任务行已不存在（孤儿）或任务可删时移除；任务非终态/带租约/DB 查询失败/目录名非 UUID 一律保留。
  - **可查询状态**：计数器 `retention_inbox_deleted_total`/`retention_outbox_deleted_total`/`retention_jobs_deleted_total`/`retention_temp_dirs_deleted_total`/`retention_sweep_failures_total` 经 control-worker 既有 `/metrics` 暴露。
  - **测试**：单测 `retention_test.go`（配置默认值与显式保留、证据窗长于成功窗、`jobDeletable` 11 例矩阵——含 running/queued/带租约/缺 finished_at/未知状态全部拒删、temp 路径校验与幂等注册/删除、非 UUID fail-closed）；集成 `retention_integration_test.go` 经 SSH 隧道对 248 真实库实跑通过（`-count=2` 稳定）：一次性夹具（专用 org、专用 consumer group、专用 outbox producer、`t.TempDir()` temp root）覆盖——超期终态被清（inbox 3 天前记录、succeeded 10 天、failed/cancelled 40 天各 1 条被删）、保留期内不删（inbox/outbox/job 各窗口内行全保留）、活跃租约保护（running 任务含过期租约仍保留；已投递但带租约的 outbox 行保留）、未投递 outbox 保护（`sent_at IS NULL` 行保留）、temp 目录三分支（可删任务的目录被删、running 任务的目录保留、孤儿目录删除、非 UUID 目录保留）、清理幂等（第二次 Sweep 删除数为 0）。过程修正一处测试口径缺陷（job 幸存者数误写 6 实为 5——8 个夹具行中恰好删 3 个；非实现缺陷）。实跑后核验 248 残留为 0（organizations/inbox/outbox/jobs 四处 `retention_it_*` 计数全 0）。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 0 FAIL。**部署范围说明**：本轮不部署 248；retention 生效需要 control-worker 新二进制上线（T02 协议层消费者接线同属后续范围）。

## 阶段 6：实体与归因（P1；依赖阶段 4、阶段 5、C03）

- [x] E01（G/D）实现 Account/Device 身份空间注册、强弱标识优先级、规范化和 entity.id 生成。
  - **2026-10-02 完成并勾选（代码+单测全绿，集成测试经 SSH 隧道对 248 真实库实跑通过）**。实现为新包 `internal/entity`（`normalize.go`/`id.go`/`registry.go`），存储复用 `00007` 的 `entities` 表，新增唯一迁移 `00016_identity_spaces.sql`（`identity_spaces` 注册表 + `entities(organization_id, authority)` 到空间的复合 FK，未注册空间无法写实体；248 台账推进至 00016，checksum 与仓库文件一致，expand-only，无存量数据改动）。
  - **身份空间注册**：`Registry.RegisterSpace` 幂等——空间名规范化（小写、受限字符集）后按 (organization, name) 去重，重放返回同一 `is:` 前缀稳定 ID；同名不同 kind 报 `ErrSpaceKindConflict` fail-closed。
  - **规范化（fail-closed）**：SID 小写+格式校验；GUID/device UUID 去花括号小写校验 8-4-4-4-12；`DOMAIN\user` 分拆 namespace/name 双侧小写；UPN/邮箱小写（原值保留在 document）；hostname 小写去尾点、不擅自补域；IP 走 `netip` 标准文本形式。未知 kind、空值、畸形值一律拒绝。
  - **强弱优先级**：强标识（sid/guid/device_uuid/agent_id）恒定胜过弱标识（ntname/upn/email/username/hostname/ip），同类两值冲突判 `ErrAmbiguousInput`；同优先级按 kind 字典序确定性裁决，与输入顺序无关。
  - **entity.id**：按 contracts/ids.md 的长度前缀哈希从 (tenant, entity_type, authority, canonical_key) 派生 `ent:` + sha256，同一真实身份任意次注册得到同一 id；跨租户/类型/空间/键均不冲突（单测断言）。
  - **弱标识易主**：弱标识 occurrence 语义——活跃 occurrence 内重注册幂等返回同 id；`TransferWeak` 关闭活跃 occurrence（`valid_to`，时间不晚于 `valid_from` 则拒绝；强标识不可易主）后，同一弱键再注册生成 `base#N` 新 canonical key 与新 entity.id，复用的用户名/主机名绝不并入前任。注册经 `pg_advisory_xact_lock` 串行化，并发注册同一身份只落一行同 id。
  - **测试**：单测 `entity_test.go`（规范化正例 11/反例 16、强弱裁决、同类冲突、ID 稳定性与隔离、空间名正反例）；集成 `registry_integration_test.go` 5 例经隧道对 248 实跑通过（`-count=2` 稳定）：空间注册幂等+kind 冲突、entity.id 跨注册稳定+并发 8 路单行、跨空间同键不合并、弱易主全生命周期（幂等→Transfer→新 occurrence 新 id→无活跃拒绝→强标识拒绝易主）、未注册空间/非法类型/非法标识/空标识全部 fail-closed 且零写入。夹具为一次性 `entity_it_*` organization，跑完清理，实测 248 上 orgs/spaces/entities 三处 `entity_it_%` 计数均为 0。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 32 包 0 FAIL。**范围说明**：实体注册尚无 worker/API 接线（E02 归因与后续条目的消费方）；248 未部署任何新二进制。
- [x] E02（G）实现多角色 attribution，包含 resolved/unresolved/ambiguous 和证据；event.id 不被改写。
  - **2026-10-12 完成并勾选（单测+经 SSH 隧道对 248 真实库集成验收通过）**。实现在 E01 的 `internal/entity` 包内增量：`attribution.go`（引擎）+ `roles.go`（UIM 角色提取）。**无新迁移**——投影落库复用 `00007` 已有的 `entity_attributions` 表（其 CHECK 约束 `resolved ⇔ entity_id IS NOT NULL` 与三态语义天然一致），248 台账仍为 00016，未变动。
  - **多角色独立归因**：`RolesFromUIM` 从 UIM 事件提取角色观察——`user`→actor、`user.target`→target、`group`→group、`host`→host、`source`/`destination`→source_device/destination_device（账户角色由调用方给账户身份空间，设备角色给设备身份空间；空间名是部署映射、不从事件正文取信）。`Attributor.Attribute` 对每个角色独立输出 `RoleAttribution{AttributionID, EventID, Role, State, EntityID?, Confidence, RuleVersion, ValidFrom/To, Evidence}`；`AttributeEvent` 为组合便捷入口。角色映射版本 `RoleMappingVersionV1="1.0.0"` 折入 attribution.id。
  - **三态与歧义规则**：每角色把标识逐一经 E01 `Normalize` 规范化后只读查询 `entities`（同 canonical key 含 `#N` occurrence 全部读回），裁决核心 `adjudicate` 是纯函数：**resolved**=事件时刻（`@timestamp`）落在且仅落在一个 occurrence 的 `[valid_from, valid_to)` 内，置信度强标识 1.0/弱标识 0.8；**unresolved** 带可查原因码（`no_matching_entity` / `no_active_occurrence_at_event_time` / `identifier_invalid` / `identity_space_unregistered` / `no_identifiers`）；**ambiguous**=事件时刻命中多于一个实体——强弱标识分别解析到不同实体（`strong_weak_conflict`）或同一弱键多个活跃候选（`multiple_active_candidates`），**候选全部保留在证据里，绝不"最近一个"覆盖**（设计基线 §5 原文要求）。
  - **证据可审计**：`Evidence.Identifiers[]` 逐标识记录 kind、原始值、规范化输出、强弱、命中的候选实体（entity_id/canonical key/强度/revision/有效区间）或规范化失败原因；`Adjudication[]` 记录裁决路径（如 `resolved_by_strong_identifier`、`strong_identifier_priority_over_weak`、`occurrence_outside_event_time:<key>`、`candidates_preserved_not_overwritten`）；`Reason` 携带原因码。落库时 `resolution_snapshot`（state/entity/canonical key/rule version）+ `evidence` JSONB 同行持久化。
  - **event.id 不被改写**：引擎对事件只读——`Attribute`/`AttributeEvent` 入参的 event.id 原样透传到每条 attribution 的 `EventID`，无任何写事件本体的代码路径；单测断言归因前后事件 map JSON 逐字节一致，集成测试断言落库行 `event_id` 与输入逐字相同。
  - **attribution.id**：按 contracts/ids.md 由 (event id, entity snapshot, role mapping version) 派生 `att:` + sha256；v1 snapshot 组成已补记进 ids.md（`role|state|entity_id|canonical_key|sorted_candidate_ids`）。同事件同角色同事件时刻重算同 id；`Store` 按 (organization, attribution_id) `ON CONFLICT DO NOTHING` 幂等。
  - **弱标识易主后新旧事件归因不同**：集成测试实测——web01 注册得实体 A，`TransferWeak` 后再注册得实体 B（`web01#2`）；t4 的事件 resolved 到 B，而 t1 的旧事件重算仍 resolved 到 A，历史不被改写。
  - **测试**：单测 `attribution_test.go` 15 例（三态各自正反例、强弱冲突与多候选歧义、证据完整性、归因 id 确定性/作用域/格式、角色提取正反例、event.id 不可变断言）；集成 `attribution_integration_test.go` 4 例经隧道对 248 实跑通过（`-count=2` 稳定）：UIM 事件五角色端到端（actor/target/host resolved、source/destination unresolved 带原因、证据含规范化输入输出、Store 幂等重放零新增）、弱易主前后归因差异、强弱冲突 ambiguous 落库且 entity_id 为 NULL、未注册空间/非法标识/空 event.id/空角色/非法租户/非法实体类型全部 fail-closed 且零写入。夹具为一次性 `attr_it_*` organization，跑完清理，实测 248 上 orgs/attributions/spaces/entities 四处残留计数均为 0。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 32 包 0 FAIL。**范围说明**：归因尚无 worker/Topic 接线（消费 UIM Topic、按 entity.id 分区产出属 E04；实体/归因/关系的 ES 投影属 E05）；248 未部署任何新二进制。
- [x] E03（G/D）实现时态关系、有效区间、规则快照与冲突处理；缺少关系不阻止单实体特征。
  - **2026-10-12 完成并勾选（单测+经 SSH 隧道对 248 真实库集成验收通过）**。实现继续在 `internal/entity` 包内增量：`relation.go`（时态关系存储/查询/冲突处理）+ `rulesnapshot.go`（规则快照注册表）+ `features.go`（单实体特征视图与降级语义）。新增唯一迁移 `00017_rule_snapshots.sql`（`rule_snapshots` 不可变规则版本表 + `entity_relations` 的活跃关系/反向时态索引；expand-only，无存量数据改动），248 台账由 00016 推进至 00017（沿用 `.runtime/apply00016` 同款一次性 Go 应用器，advisory lock + 单事务 + checksum 账本，checksum 与仓库文件一致）；关系本体落库复用 `00007` 已有的 `entity_relations` 表（`rel:` 主键格式、双端复合 FK、`valid_to > valid_from` CHECK 均在位）。
  - **时态关系**：`RelationStore.AssertRelation` 断言一条带 `[valid_from, valid_to)` 有效区间的实体间关系（`member_of`/`belongs_to` 多值、`assigned_to`/`managed_by` 单值——单值型同一 (from,type) 任意时刻至多一个活跃 `to`）；relation.id 按 contracts/ids.md 由 (tenant, from, type, to, event id, resolution snapshot=`relation_type|relation_mapping|rule_version|asserting_event_id`) 派生，同一事件重放派生同一 id 幂等返回；`CloseRelation` 关闭区间（同 valid_to 重关幂等、不同时间 `ErrRelationAlreadyClosed`、不晚于 valid_from 拒绝）；`TransferRelation` 单事务内完成易主——旧区间与新区间在同一时刻闭/开，边界瞬间恰好一个持有者（集成测试断言 transfer 前一纳秒视图归旧主、转移时刻起归新主）；`RelationsAt` 给出某时刻的双向时态视图（`DirectionOutgoing/Incoming/Both` + 类型过滤），半开区间语义与 E01 occurrence/E02 归因判定逐点一致（valid_from 时刻可见、valid_to 时刻不可见，集成测试逐边界断言）。
  - **冲突处理（fail-closed）**：写路径按 (org, relation_type, from_entity) 取 `pg_advisory_xact_lock` 串行化，同事务内做区间重叠检查——同 (from,type,to) 有重叠区间但由不同事件断言、或单值型 (from,type) 重叠区间内出现不同 `to`，一律 `ErrRelationConflict` 拒绝且零写入；多值型允许不同 `to` 并存。纯函数 `intervalsOverlap` 为半开区间判定（相接不重叠），单测 7 例覆盖。未知关系类型/未注册规则版本/端点实体缺失/置信度越界/空时间全部 fail-closed 且零写入（集成测试断言前后行数不变）。
  - **规则快照**：`RuleSnapshots.Register` 幂等注册 (org, kind, version) 的规则内容（JSON 紧凑化后 SHA-256）；同版本同内容重放返回同一快照，**同版本不同内容 `ErrRuleSnapshotConflict` 拒绝、已存规则不变**——规则升级必须新版本号，历史结论引用的规则版本永远可读回（`Load` 未注册版本即硬错误）。`AssertRelation`/`TransferRelation` 要求引用的 `relation_mapping` 版本已注册，否则 fail-closed。
  - **缺少关系不阻止单实体特征**：`FeatureAssembler.SingleEntity` 产出窗口内按角色的 resolved 归因计数（特征路径本体，失败才硬错误）；关系查询仅为窗口末时刻的增强上下文——查询失败时降级返回 `RelationsDegraded=true`+错误说明而**计数不受影响**，无关系时正常返回空关系列表（不算降级）。单测覆盖纯组装核的三分支（失败降级/无关系正常/有关系增强），集成测试在真实库上跑通无关系→有关系→关系查询失败注入三条路径。
  - **测试**：单测 6 例（relation.id 确定性/六维作用域隔离/格式、半开区间重叠矩阵、类型基数、规则 kind/version 校验、快照内容规范化哈希、降级组装三分支）；集成 `relation_integration_test.go` 5 例经隧道对 248 实跑通过（`TUBA_TEST_DATABASE_URL` 门控）：时态视图全边界（assert→时点前不可见/valid_from 起双向可见/类型过滤/事件重放幂等/close 后半开边界/重关闭幂等与冲突）、冲突 fail-closed（单值重叠拒、同三元组异事件拒、多值并存、五类非法输入零写入）、易主（原子闭开、边界视图、无活跃/同持有者/多值型三类拒绝）、快照不可变性（幂等重放、改写拒绝且原内容读回不变、未注册版本/非法引用拒绝、行数核对）、降级路径（归因计数在无关系/有关系/关系查询失败三态下逐字节一致）。夹具为一次性 `rel_it_*` organization，跑完清理，实测 248 上 orgs/relations/snapshots/attributions/entities/spaces 六处 `rel_it_%` 计数均为 0，且 `entity_relations`/`rule_snapshots` 全表为 0（无生产数据改动）。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 0 FAIL。**范围说明**：关系尚无 worker/事件驱动接线（由 E04/E05 消费产出与投影）；248 未部署任何新二进制。
- [x] E04（G）输出按 entity.id 分区的 attributed 消息，多角色具有独立贡献键；支持事件级未归因检测输入。
  - **2026-10-12 完成并勾选（单测+经 SSH 隧道对 248 真实库集成验收通过；Kafka 层用 fake writer，未触碰 248 任何 topic）**。**无新迁移**（248 台账仍为 00017，未变动）。产出路径三段：消息契约 `contracts/events/attributed-event/1/{schema.json,contract.md}`（draft 2020-12；`state=resolved` 强制 `entity_id`、非 resolved 禁止 `entity_id` 且强制 `reason`，以 if/then 编码进 schema）、纯函数扇出核 `internal/entity/attributed.go`（`ContributionKey`+`Contributions`，entity 包保持无 Kafka 依赖）、worker 包 `internal/entityworker`（`Worker` 镜像 normalizer 形态：Consumer/Processor/Writer 三接口；`AttributionProcessor` 组合 E02 的 `AttributeEvent`+`Store`）与入口 `cmd/tuba-entity-worker`（8 个 UIM 域 topic 各一 reader、消费组 `tuba-entity-worker-<domain>-<namespace>` 遵循 topics.v1.json 既定声明，共享一个 `kafka.Writer`（Hash balancer、RequireAll）写 `tuba.attributed.events.v1`，PG 经 `pgutil.NewPool`，身份空间由 `ENTITY_ACCOUNT_SPACE`/`ENTITY_DEVICE_SPACE` 部署映射供给；`internal/config` 新增 `KAFKA_ATTRIBUTED_TOPIC`（默认 `tuba.attributed.events.v1`））。
  - **按 entity.id 分区**：每角色归因产出一条独立 contribution 消息，Kafka key = `<org>:<entity.id>`（`ent:` 前缀哈希），正文 `partition_key` 与 key 逐字一致供下游无 key 消费者使用。同一实体的所有相关消息落到同一分区，为 F02 的 per-entity 有界窗口状态铺路。
  - **多角色独立贡献键**：选型为"每个角色一条贡献消息"（而非一条消息多键）——一条五角色事件扇出五条消息，每个 resolved 实体在自己的分区里看到这条事件的贡献；两角色命中同一实体时两条贡献共享同一 key（同分区有序）。
  - **事件级未归因检测输入**：unresolved/ambiguous 归因同样产出消息、绝不丢弃或静默过滤，键为 `<org>:unresolved:<state>:<reason>`（reason 为 E02 的稳定原因码；缺失时确定性回退 `unknown_reason`）——独立键命名空间可被下游消费且不污染任何实体分区（`ent:` 前缀使两类 key 结构上不可能冲突；schema 的 if/then 约束与 `ContributionKey` 的 fail-closed 校验双保险：resolved 无 `ent:` id、非 resolved 携带 entity id 一律硬错误）。
  - **投递语义**：与 normalizer 同口径的 at-least-once——所有 contribution 被 writer 接受后才提交输入 offset，崩溃重投由下游按 `attribution_id`（E02 稳定 id，随消息头 `attribution-id`/`role`/`state`/`event-id`/`schema-version` 携带）去重；证据全文随消息携带，未归因消息无 PG 也可分诊。写失败/归因失败/输入不可解码全部 fail-closed：不发布、不提交。topics.v1.json 的 `tuba.attributed.events.v1` 条目已补记 key 语义与 contract 指针。
  - **测试**：单测 `internal/entity/attributed_test.go` 6 例（同实体同 key/异实体异 key/租户入 key、unresolved/ambiguous 键格式且绝不等于实体键、缺 reason 确定性回退、四类 fail-closed 拒绝、五角色扇出完整性含两角色同实体共享 key、JSON round-trip 逐字节一致+正文 partition_key 可见+非 resolved 无 entity_id）；单测 `internal/entityworker/worker_test.go` 4 例（fake consumer/writer/processor：每归因恰一条消息、key==partition_key、五消息头齐备、write 先于 commit、写失败不提交、归因失败不发布不提交、垃圾输入 fail-closed）；集成 `worker_integration_test.go` 经 SSH 隧道对 248 真实库实跑通过——真实 Attributor+Store、fake Kafka：五角色消息齐备（actor/target/host 按各自注册实体 id 分区、source/destination 未归因两条不丢且键为 `unresolved` 命名空间、event.id 逐字未改写、归因投影落库 5 行、输入恰提交一次）。夹具为一次性 `attrmsg_it_*` organization，跑完清理，实测 248 上 orgs/attributions 残留计数均为 0。`go build ./...`、`go vet ./...` 干净，全量 `go test ./...` 0 FAIL（`internal/launcher` 的 `TestSingleServiceControlLifecycle` 在本机偶发超时、与本次改动无关，单跑 `-count=1` 通过）。**范围说明**：本轮不部署 248（新二进制上线需另配 SCRAM 身份/精确 ACL 与 launcher 登记）；真实 broker 投递路径与既有部署一致，未自建测试 topic。
- [x] E05（G/D）扩展 sink 写实体/归因/关系投影，revision 防止旧值覆盖；实体来源/历史可追溯。
  - **2026-10-12 完成并勾选（单测 httptest fake ES 全绿 + 对 248 真实 ES 冒烟验证通过，测试资产跑完即删零残留）**。**无新迁移**（PG 台账仍为 00017，未变动；纯 ES 投影侧扩展）。实现在 `internal/sink/projection.go`（沿用既有 `*Elasticsearch` sink 与 `PermanentIndexError` 语义，entity 包零依赖回指 sink）。
  - **投影形态（读多写少最新状态 + 可追溯历史，双层）**：① 状态投影——`ueba-entities-<ns>`（实体主档：canonical 标识/类型/身份空间 authority/occurrence 有效区间/registry revision/最近归因 `last_attribution.id+revision`）、`ueba-relations-<ns>`（关系：双端实体/类型/置信度/规则版本/时态区间/lifecycle revision），按 `entity.id`/`relation.id` 作 `_id`，索引模板 `tuba-entity-projection-v1`/`tuba-relation-projection-v1`（新文件 `elasticsearch/{component,index}-template-{entity,relation}-projection-v1.json`，仿 anomaly 手写模板惯例，strict 映射，已接入 `scripts/apply_elasticsearch_assets.sh`；新模式 `ueba-entities-*`/`ueba-relations-*` 与既有模式零重叠，向后兼容，不装 ILM——保留仍是 O05/A03 显式操作）；② 历史投影——sink 自管追加型日期索引 `tuba-v1-entity-history-<ns>-g1-<day>`、`tuba-v1-relation-history-<ns>-g1-<day>`、`tuba-v1-attributions-<ns>-g1-<day>`（读别名 `logs-ueba.entity-history/relation-history/attributions-<ns>`，沿用 quarantine sink 的内联 strict 映射建索引惯例），实体/关系每个 revision 一帧（`_id=<id>:<revision>`），归因按 `attribution_id` 不可变落一帧。
  - **revision 防旧值覆盖（ES external version 选型）**：状态写入走 `PUT _doc/<id>?op_type=index&version_type=external&version=<revision>`——ES 只在 version 严格更大时接受，乱序/迟到写入返回 `version_conflict_engine_exception`；sink 读回存贮文档裁决：内容哈希相同→同 revision 幂等成功（at-least-once 重放安全）；version 相同内容不同→`PermanentIndexError{PROJECTION_REVISION_CONFLICT}`（呼应 F07 同 revision 内容冲突口径）；version 更旧内容不同→新类型 `StaleRevisionError`（`IsStaleRevision` 判定，含 existing/incoming revision），**存贮的较新值不被覆盖、stale 快照也不进历史索引**；消费方语义为丢弃不重试（revision 决定顺序而非到达顺序，与 F06 revision/retracted 呼应）。历史索引 `create` 语义 + 409 读回内容哈希裁决幂等/冲突。
  - **来源/历史可追溯**：三类投影文档均携带 `source.event_id`/`source.raw_event_id`（归因另带 `event.id/time/domain` 与 partition_key、证据 JSON），契约校验缺 `event.id` 引用 fail-closed；实体主档带 `last_attribution.id+revision` 可跳转到归因投影，历史索引按 `<id>:<revision>`/`attribution_id` 逐帧留痕，从投影可回溯到来源事件与每一历史版本。
  - **错误语义**：传输失败与 429/5xx 保持可重试普通错误（不包装成 Permanent/Stale）；契约校验失败（缺身份/租户/revision<1/缺时间/缺 event.id/区间非法/证据非 JSON）fail-closed `PROJECTION_CONTRACT_INVALID` 且零 HTTP 请求；批量/文档 4xx 拒绝为 `PermanentIndexError`。
  - **测试**：单测 `projection_test.go` 10 例（自实现 fake ES——真实版本冲突语义而非脚本化应答）：实体状态+历史双写且快照一致、别名正确、外部版本号入请求；**乱序 rev5→rev2 拒写且存贮值/历史索引均不被污染（red-line 锁定）**；同 revision 幂等（状态不重写、历史不重复）；同 revision 不同内容 `PROJECTION_REVISION_CONFLICT`；关系 lifecycle（开区间 rev1→关闭 rev2 生效→迟到的 rev1 重开请求被 stale 拒绝、关闭状态不被回滚、两帧历史俱在）；归因可追溯字段完整（event.id/raw_event_id/partition_key）+重放幂等+同 id 异内容永久冲突；五类非法契约零请求 fail-closed；ES 不可达与 503 均可重试且重试后成功。真实 ES 验证：对 248 以 `e05test-*` 前缀自建模板+索引冒烟——rev3→201、rev5→200、迟到 rev4→409 `version_conflict_engine_exception`、同 rev5 异内容→409、读回存贮文档仍为 rev5 原内容且 source 引用完整、模板 strict 映射生效，跑完删除全部测试索引与模板并核验 248 上 `e05test*` 索引/模板/组件三处计数为 0（凭据不进报告/提交）。`go build ./...`、`go vet ./...` 干净，全量 `go test ./... -count=1` 33 包 0 FAIL。**范围说明**：投影写入尚无 worker 接线（消费 `tuba.attributed.events.v1`/registry 变更产出投影属后续条目）；248 未部署任何新二进制，`apply_elasticsearch_assets.sh` 的投影模板安装待下次资产运维执行。至此 E 系（E01–E05）全部完成。

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
- 2026-09-29 当前 TUBA `source_instances` 列表为空。248 `release_bundles` 原有 `zeek-validation-20260927-v1` staged 记录的 manifest 是空对象 `{}`，不能作为 Windows Security 语义 release；本轮没有改动该旧记录。Windows Security bundle 已补齐并预置 248；T04 API、迁移及 publisher 引导已部署，实际 release row 仍待有效操作员登录后按审计流程创建。发布完成后，再通过来源 API 分别登记 139/169，创建独立 Kafka SCRAM 身份/精确 ACL、更新 adapter allowlist，最后完成 Kafka→receipt→Raw ES→DIP/UIM→标准 ES 的逐段数量及重放验收。2026-09-30 更新：`windows-security-1.0.0` release 已完成登记并推进至 active，审计链完整，见 T04 条目；后续 139/169 来源登记仍按上述顺序执行。**2026-10-01 复核（T04 后续）：139/169 登记事实上已于 2026-09-29 19:48 全部完成，本条"仍按上述顺序执行"已过时，本轮只做读回核验，未重复登记、未新建任何身份。** 核验证据：① PG `source_instances` 两条 active 记录——`src_a6b2f9d8a890cf30255a27103c90af47`（139，AD 域控）与 `src_03329669e117108230ae960fefef700a`（169，组成员），tenant_a / windows.security / release_id=`windows-security-1.0.0` / rate_limit=500；`source_contexts` 各一条不可变上下文（`ctx_a6b2f9d8…`、`ctx_03329669…`）；`audit_events` 两条 `source.register`，actor=`26a64c67-…-201a8657ec03`，各带独立 request_id，发生于 19:48:02–03 +08:00。② Kafka：SCRAM-SHA-512 用户 `tuba-windows-139`/`tuba-windows-169` 已存在；各自仅对唯一 source topic 持有 WRITE+DESCRIBE（`tuba.source.ctx_a6b2f9d8….v1` ↔ 139、`tuba.source.ctx_03329669….v1` ↔ 169），`tuba-zeek-source-adapter` 对两 topic 持 READ+DESCRIBE。③ adapter allowlist `/opt/tuba/collector-live/pipeline/config/source-adapter.json` 含全部 6 个绑定（4 个 zeek + 上述 2 个 Windows ctx topic）。④ 数据面：adapter 消费组 `…-3a5f5426333adfdd-…`（139）与 `…-91f5ede6aee00ba0-…`（169）lag=0、有活跃 member；ES `logs-ueba.raw-tenant_a` 共 35,989 条，按 `source_context_id` 分桶 139 侧 31,459 / 169 侧 4,514，两侧 `received_at` 距采样时刻均为秒级。**发现并修复一处线上缺陷（未部署）**：`GET /api/v1/sources` 当前恒返回 503 `source registry unavailable`——`internal/control/sources.go` 的 `ListSources` 在 00014 迁移加入 `state` 列后，SELECT 含 `si.state` 但 `rows.Scan` 少了对应目标，导致每次列来源都扫描失败；已在本仓库修复并补集成测试 `internal/control/sources_integration_test.go`（旧代码会因扫描错误失败），`go build`/`go vet`/`go test ./...`（26 包 ok、0 FAIL）通过。**修复已于 2026-10-01 部署到 248（用户批准后执行，单服务路径，非 manifest-wide restart）**：以仓库工作树（基于 `cc5b693` 加本修复，`vcs.modified=true`）构建 linux/amd64 `tuba-api`，SHA-256 `987ad32e…6906`；旧二进制（sha `32b235aa…103d`）先归档至 `21:/opt/tuba-backup/248/cleanup-20260930/bin/tuba-api.pre-listfix-20261001`（归档后 sha256 复核一致），新二进制传为 `tuba-api.new` 比对哈希后替换 `/opt/tuba/bin/tuba-api`，随后 kill api 子进程（pid 1842158），Launcher 5 秒内以新二进制拉起（pid 1867697，restarts 0→1，符合预期），status 11/11 running、其余 10 个服务 pid/restarts 均未变。**验证**：操作员 token 下 `GET /api/v1/sources` 由恒 503 恢复为 **200**，正确列出两条 Windows 来源（`state=active`、`release=windows-security-1.0.0`、各自 `source_context_id`）；`/api/v1/me` 正常；`/metrics` 200；两个 Windows adapter 消费组 lag 重启前后均 0。248 本地旧二进制副本已删除（归档保留在 21）。
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

### 2026-10-01 V07 续：source-adapter 稳态吞吐上限实测（造压，首轮上限数据）

回答上一轮遗留问题：3–4 条/秒的稳态是限速配置还是能力上限，以及 COL-07b 级别突发积压多久能追平。测试在 248 实测（约 11:55–12:10 +08:00），未改 adapter 任何配置/二进制。

**限速判定（配置与代码）**：adapter 配置只有 `ingest_url` + `bindings`（`/opt/tuba/collector-live/pipeline/config/source-adapter.json` 读回，6 绑定），无 rate_limit/batch/wait 类参数；`internal/sourceadapter/adapter.go` 的循环为每绑定一个 goroutine 严格串行 fetch→HTTP receipt→同步 offset commit（`cmd/tuba-source-adapter/main.go:107-138`，kafka.Reader `CommitInterval:0` 即逐条同步提交）；PG `source_instances.rate_limit` 四条 zeek 来源均为 1000 条/秒（Windows 两条 500），不会在实测量级触发。**3–4 条/秒不是配置限速。**

**实验 A（receipt 去重路径，不触下游）**：导出 ssl 源 topic（`ctx_5fadf1a6…`）当时全部 1372 条存量（12h 窗口内，receipt 均在 2 天保留内），原样灌回该 topic 尾部两遍 = 2744 条有效消息。receipt 去重（`GetOrCreate` 命中、kafkaAcked=true）吸收全部重放，未产生新 raw Kafka 写入、未触 ES。实测：灌完后 28 秒内消费约 2850 条（含实时流量），峰值窗口 17 秒 +2083 ≈ **122 条/秒**，整体约 **95–100 条/秒/绑定**。

**实验 B（完整新事件路径，真实入库）**：同批消息改写 `log.offset += 9e8` 并加 `v07_throughput_test: "20261001"` 标记，得 1372 条全新 `raw_event_id` 消息灌入同一 topic。消费组 lag 从 1372+ 到 0 用时 **33 秒**；按 fetched 计数折算单绑定吞吐 **40–60 条/秒**（稳定窗口 54–61/s）。该路径含 PG receipt 插入、ingest 同步写 raw topic 与 `MarkKafkaAcked`、offset 提交，是完整的生产路径。闭合校验：ES raw alias 中标记文档 1276 条 == 源导出中 1276 个唯一位置（1372 条里 96 条是已知的 Filebeat 活日志+归档双读位置重复，被 receipt 正确去重）；PG `ingest_receipts` 窗口内新增约 1372 行，按既有 2 天清理策略自然过期。

**聚合余量（自然观察）**：实验 B 后约 4 分钟，21 侧归档批次自然涌入，adapter 六绑定合计 fetched 在 90 秒内 +14,333 ≈ **158 条/秒**，各组 lag 秒级归零。

**结论**：同一二进制、同一配置下实测单绑定 40–60 条/秒（完整路径）与约 100 条/秒（去重路径）——**09-30 记录的 3–4 条/秒既不是配置限速也不是能力上限**，应归因于当时 `ingest_receipts` 表膨胀（175 万行 / 2.5 GB，清理 + VACUUM FULL 之前）造成的 PG receipt 写延迟。瓶颈形态：adapter 无批量无并发，单分区单绑定吞吐上限 = 1/串行延迟（当前约 15–25 ms/条）；完整路径与去重路径的差值（~100 vs ~50/s）表明**先饱和的是 ingest 的同步段（PG receipt GetOrCreate + Kafka raw produce），而不是 Kafka fetch 或 offset commit**；PG receipt 表健康度是该链路吞吐的一阶变量，2 天清理任务必须保持运行。

**突发追平估算**：按 40–60 条/秒/绑定，COL-07b 级别的单 topic 数千条积压约 1.5–2 分钟追平；09-30 那次 11,700 条积压在今天的表健康度下约 3–5 分钟（当时拖了 49 分钟以上的真因是 receipt 表膨胀）。多绑定并行线性叠加（实测聚合 ≥158 条/秒）。

**DLQ 说明**：本轮 adapter `events_rejected_total`/`dlq_written_total` 各 +2（topic `…source-adapter.dlq.v1` offset 1399/2772，`BEAT_EVENT_INVALID`）。原因是导出文件首行混入了 kafka-console-consumer 打印到 stdout 的 `--consumer.config` deprecation 警告文本，该行非 JSON，被 adapter 本地校验拒绝进 DLQ——属测试操作瑕疵，不是链路缺陷，也不影响上述吞吐结论。

**顺带核实（非新问题）**：raw-indexer/normalizer 的旧消费组在已退役的 `…raw.v1` topic 上有冻结 lag（2060/1682，无成员），与此前记录的无后缀孤儿 adapter 组同类；现役链路走 `…raw.live2.v1`，全部消费组 lag=0。本轮结束后 11/11 服务 running、restarts 不变，六路 adapter 组 lag 全 0。

**测试数据留存**：`logs-ueba.raw-zeek_validation_20260927_001` 内有 1276 条 `payload.v07_throughput_test="20261001"` 标记文档及其下游 normalizer/standard 产物，按既有 ES 7 日 / PG receipt 2 日保留自然过期，不单独清理。

**V07 仍未勾选**：端到端 P95、PG 事务预算上限、多来源同时有压的压测本轮未做；本轮只回答了 adapter 单实例吞吐上限与追平速率。

### 2026-10-01 V07 再续：source-adapter 吞吐上限实测（自然积压法，零合成注入）

上一轮的「造压」靠向源 topic 灌入改写后的消息，虽未污染下游（receipt 去重 / 用一次性标记），但**向生产 topic 写入测试消息这一动作本身有污染风险，已被用户叫停**。本轮改用与 COL-07b 5/5 同源的**自然积压法**：暂停 adapter 让 21 侧 Filebeat 的真实日志在源 topic 里自然堆积，恢复后测追平速率。**全程未向任何 topic 灌入一条消息**，未改 adapter 任何配置/二进制，未触碰 tenant_a 链与监控栈。实测约 12:17–12:39 +08:00。

**手段侦察（Launcher 无单服务粒度）**：`cmd/tuba-launcher/main.go` 只暴露 `validate|merge-manifest|start|stop|restart|status|logs`，无单服务 stop/start；`internal/launcher/manager.go:255` 的 `runService` 是 `for ctx.Err()==nil` 的无限重启循环（退避 1s→30s），杀死子进程只会触发自动拉起，**无法用它单独停住一个服务**。选定的最小影响手段是：对**唯一一个 adapter 进程**发 `SIGSTOP`——`cmd.Wait()` 阻塞在僵直进程上，Launcher 视其「running」而不会重启；指标端口随进程整体僵直，因此采样用 `curl --max-time`；进程内 6 个绑定 goroutine 一起冻结，Kafka 端凭 24h 保留、21 侧凭 Filebeat 磁盘队列缓冲，不丢数据。窗口结束用 `SIGCONT` 原样恢复，无需重启、无需改配置、不波及其他 10 个数据面服务。脚本以 `trap 'kill -CONT' EXIT` 兜底，保证不会把进程留在冻结态。

**基线（SIGSTOP 前，t=12:17:23）**：六路源 topic log-end = conn 1,797,039 / dns 481,370 / http 253,543 / ssl 6,904 / win(`0332…`) 8,478 / win(`a6b2…`) 32,793，合计 2,580,127；adapter `fetched=committed=56,802`、`accepted=56,800`、`rejected=2`、`dlq_written=2`；六个 adapter 组 lag 全 0；DLQ log-end 10,472；`ingest_receipts` 1,257,882 行。**自然到达速率**（90 秒被动观测）：合计 **4.41 条/秒**（conn 2.73、dns 0.76、http 0.49、ssl 0.23、Windows 两路 0.20），即 09-30 记的「稳态 3–4 条/秒」就是到达速率本身，因 lag 恒 0、消费速率被迫等于到达速率，**根本不是处理上限的表现**。

**造出的积压**：SIGSTOP 于 12:17:29、SIGCONT 于 12:22:29，冻结 **300 秒**。恢复瞬间六路 lag 合计 **≈1,350 条**（≈5.1 分钟的真实数据）。积压落在 conn 约 810、dns 约 230、http 约 150、ssl 约 70，Windows 两路可忽略。

**追平曲线**（SIGCONT 后每秒采样 adapter `events_fetched_total`，`t=0` 为 SIGCONT）：

| t (s) | fetched | 备注 |
| --- | --- | --- |
| 0–6.2 | 56,802→56,806 | **重组静默 ≈6.5s**：冻结 300s 超过 broker session 超时，kafka-go 被移出消费组后重加入 |
| 7.2 | 56,825 | 开始消费 |
| 8.2–11.3 | 56,988→57,383 | 冷启动峰值，8.2→9.3 的 1.1s 内 +169 ≈ **154 条/秒** |
| 11.3–17.3 | 57,383→57,760 | +377/6.0s ≈ 63/s |
| 17.3–26.5 | 57,760→58,253 | +493/9.2s ≈ **54 条/秒** |
| 27.5 起 | 58,258 后回落至 4–6/s | 追平，此后仅跟踪到达 |

**实测吞吐**：纯消费窗口 t=7.2→27.5（20.3s）内 fetched +1,456，扣除同期到达约 120 → **积压追平速率 ≈ 66–72 条/秒（聚合，6 绑定并发）**，瞬时峰值 >150 条/秒；含 6.5s 重组静默的端到端追平 ≈ 47–53 条/秒。**1,350 条积压 28 秒追平**（其中已含 6.5s 重组停顿）。据此外推 09-30 那批 **11,700 条 ≈ 3–4 分钟**，与 2026-10-01 造压轮「3–5 分钟」的估算一致。**3–4 条/秒 既不是配置限速（配置无 rate_limit/batch/wait），也不是能力上限（实测 66–72/s，差 15–20 倍）**。

**瓶颈层判定（按证据逐层排除）**：

- **不是 Kafka fetch / offset commit**：整段追平 `kafka_fetch_retries` 保持 1（未增）、`stall_events` 保持 1（未增）、`consumer_replacements` 保持 1（未增）；唯一代价是 300s 冻结诱发的约 6.5s 组重组，属 `SIGSTOP` 手段的固有开销，不是稳态瓶颈。
- **不是索引 / ES 侧**：追平结束后 raw-indexer、normalizer、standard-indexer、quarantine-indexer 各现役组 lag 全 0（network 组瞬态 4），ES `status=green`、unassigned 0——索引侧完全吸收了这次突发，与既有「raw-indexer 约 250 条/秒余量」的记录吻合。
- **adapter 自身无批量无并发，单绑定串行 fetch→HTTP receipt→同步 commit** 是结构性上限（`internal/sourceadapter/adapter.go:120-205`，`CommitInterval:0`）。聚合 66–72/s、6 绑定并发，等价单绑定约 15–30 ms/条的串行延迟，与 2026-10-01 轮测得的 40–60 条/秒/绑定同量级但本轮聚合值更低，符合「共享同步段先饱和」的形态。
- **共享同步段 = adapter→ingest 的 HTTP POST（PG `ingest_receipts` GetOrCreate + 同步写 raw topic + `MarkKafkaAcked`）**：本轮 `ingest_receipts` 在 448s 内 +1,979 行、adapter fetched +1,983，**逐条对齐、无背离**，说明 PG receipt 表当前健康（1.26M 行，远低于 09-30 的 175 万行/2.5GB）并未成为限速点。因此当前一阶限制是 adapter 逐条串行的设计本身，其上限由 ingest 同步段的往返延迟决定；PG receipt 表健康度仍是该链路吞吐的一阶变量，2 天清理任务必须保持运行。

**恢复校验（红线）**：结束后六个 adapter 组 lag 全 0、DLQ log-end 仍为 10,472（**零新增**）、`rejected`/`dlq_written` 保持 2；11/11 服务 running，`zeek-source-adapter` **pid 1842191 不变、restarts=0**（全程未重启）；`api` 的 `restarts=1` 发生在 11:26（早于本轮 12:17，与本次测试无关）。下游现役组 lag 全 0。tenant_a 链与第二个 Launcher（监控栈 `/etc/tuba/tuba-monitoring.json`）全程未动。

**复现方法**：`PID=$(pgrep -f '^/opt/tuba/collector-live/pipeline/bin/tuba-source-adapter$')`；`kill -STOP $PID` 保持 N 秒（本环境到达 4.4 条/秒，N=300 得 ≈1,350 条积压，要凑 3,000–8,000 条需 N≈680–1,800 秒，**超出「<5 分钟停机窗口」约束，故本轮按 5 分钟上限执行，积压量低于目标值但已足以定性上限**）；用 `kafka-get-offsets.sh`（`JAVA_HOME=/opt/adms/adms-jdk`，`--command-config /opt/tuba/collector-live/kafka/admin.properties`，broker 10.6.68.248:29292）采源 topic log-end，同时 `curl --max-time 5 127.0.0.1:19185/metrics` 采 `events_fetched_total`；`kill -CONT $PID` 后按 1s 采样至 fetched 停止跃升（lag 归 0）即为追平点。**采样脚本必须以 trap 保证 SIGCONT**，且不得向任何 topic 生产消息。

**V07 仍未勾选**：本轮只补强了「积压恢复速率」一项，且积压量（≈1,350 条）低于预设的 3,000–8,000 目标（受 5 分钟停机窗口与 4.4 条/秒到达速率共同限制）。**端到端 P95、峰值 EPS、内存/PG 事务预算上限、多来源同时有压**仍未测；据此 V07 保持未勾选。

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
4. **节点剩下的分配余量很薄，仍未解决。** 水位解耦把可用区间从 0 个点扩到 4 个点（71% 常态 → 75% 才停分配），但 VG 已无空闲 extent，扩容只能加盘。占用一旦持续超过 75%，**新分区与索引创建会再次延迟**。加盘仍未做（2026-09-30 清理已把根盘从 75% 降到 64%，见 COL-07b 清理记录，余量约束暂时缓解）。
3. **PG 恢复演练无法执行**：TUBA 数据库身份没有 CREATEDB 权限，恢复需独立实例或具备建库权限的运维身份。
4. 未核对 Raw/标准/派生/案件水位，未实测 RPO/RTO。

**过程中造成并已修复的问题，记录以免重演**

- 本地快照清理与 cron 均以 `env -i` 最小环境验证过启用路径（cron 不加载 `/root/.bashrc`，缺 `LD_LIBRARY_PATH` 时 psql 会因找不到 libpq 失败）。

### 2026-09-30 监控缺口：消费组白名单漏掉整条 tenant_a 链路

排查"lag 数值冻结"时查出两件事，都不是 lag 本身的问题。

**一、当前量最大的接入路径没有 lag 监控。** kafka_exporter 的 `--group.filter` 是一份**手工白名单**（设计如此：退役代次会留下"有已提交 offset、无成员"的组，lag 冻结不动，若放开成 `tuba-.*`，`TubaKafkaConsumerInactiveWithBacklog` 会在它们身上常驻误报）。这份白名单共写在三处——`scripts/manage_tuba_monitoring.py` 的 `MONITORED_CONSUMER_GROUPS`、`rules/kafka.yml` 的两条告警、Grafana 面板查询——**三处都只列了 `zeeklive20260927*` 的组，一个 tenant_a 组都没有**。漏掉白名单不会报错，只会静默不监控：Windows Security 那条链路（09-29 接入、当前主要增长来源）没有任何 lag 告警，而安静的 Zeek 链路全程有。已补齐：受监控组由 11 个增至 19 个，其中 tenant_a 相关 8 个。

**二、白名单必须排除退役代次，否则立刻误报。** 第一版把三个无后缀的 `tuba-source-adapter-<hash>`（`c169402d…`、`d1ff4e04…`、`395791423…`）当成 tenant_a 的适配器加了进去，"有积压且无成员"的判定随即命中这三组（lag 14176 / 5575 / 303）。核查确认它们是**已撤销的 placeholder 来源**留下的孤儿组：`CONSUMER-ID`/`HOST` 均为 `-`（无成员）、committed offset 30 秒内一动不动，且它们消费的 `ctx_6000…/7000…/8000…` **不在 `source_instances` 里**；真正承载 Windows 数据的是带 `-zeeklive20260927r2` 后缀的 `3a5f5426333adfdd` 与 `91f5ede6aee00ba0`（lag=0、有成员），而这两个此前也不在白名单里。已改为按后缀模式匹配（`tuba-source-adapter-[0-9a-f]{16}-zeeklive20260927r2`），既覆盖新注册来源又排除孤儿。**验证：19 个组导出、三个孤儿组导出 0 条序列、"无成员且有积压"命中 0、三条 Kafka 告警均 inactive。**

**遗留**：~~白名单多处重复（含 Grafana）~~ **已集中生成（2026-10-09）**：唯一手工副本 `deploy/observability/single-node/monitored-consumer-groups.txt`，`scripts/generate_monitoring_allowlist.py` 幂等重写三处产物（`manage_tuba_monitoring.py` 常量块、`kafka.yml` 告警 expr、Grafana 面板 JSON），`--check` 校验漂移（exit 1 报名字）。生成后三处与 248 现网（kafka-exporter `--group.filter`、prometheus rules、grafana json）实测逐字一致，线上零改动。孤儿消费组本身未删除（保留其 offset 作为证据），因此 `--all-groups` 排查时仍会看到冻结 lag，RUNBOOK 的 TubaKafkaLag 已写明如何区分。

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

### 2026-09-30 COL-07/V02 非破坏性子集（5/5）：采集端断网后补齐 —— 通过

**要验的不变量**：采集端（21）到 Kafka（248:29292）的网络中断期间，Zeek 持续产生的记录不得丢失；恢复后 Filebeat 磁盘队列必须把积压补齐，下游 offset 追平，且不产生重复文档。

**方法**：在 21 上用一条精确 iptables 规则切断四个 Filebeat 到 Kafka 的唯一出口，240 秒后删除同一规则：

- 阻断：`iptables -A OUTPUT -d 10.6.68.248 -p tcp --dport 29292 -j REJECT`（21:20:12.327 执行，规则 `REJECT tcp -- 0.0.0.0/0 10.6.68.248 tcp dpt:29292`）
- 回滚：`iptables -D OUTPUT -d 10.6.68.248 -p tcp --dport 29292 -j REJECT`（21:24:12.363 执行，实际阻断 240.0 秒）
- 执行前确认 OUTPUT 链为空、无同名残留；恢复后读回 OUTPUT 链已空。只动 OUTPUT 链、单条规则、一条命令即回滚；INPUT/FORWARD/DOCKER 各链未触碰。

**观测**（时间均为 21/248 本机 CST）：

| 观测量 | 阻断前（21:16 基线） | 阻断中 | 恢复后 |
| --- | --- | --- | --- |
| 四路 Filebeat 磁盘队列（spool 段字节，近似值） | 无 spool 文件 | +91s 时 conn 2.0 MB / dns 7.2 MB / http 0.88 MB / ssl 9.8 MB | 解除后 46s 峰值 conn 2.9 / dns 7.4 / http 1.1 / ssl 9.8 MB（合计约 21 MB） |
| 四个 r2 source-adapter 消费组 lag | 全部 0 | 21:24:11（解除前 1s）conn topic 末位 1,507,911，冻结 | **解除后 61 秒内四路全部 lag=0**（21:25:13 首次采样即 0），21:34:45 复核 lag 0/1（瞬时在途）、四组均有活跃 member |
| source topic 末位 offset（conn/dns/http/ssl） | 1,507,511 / 410,055 / 220,263 / 142,109 | — | 21:34:45：1,510,373 / 410,821 / 220,684 / 142,332（窗口内 +2,862 / +766 / +421 / +223） |
| ES alias `_count`（raw / network / dns / web / tls / quarantine） | 2,012,951 / 1,318,634 / 348,242 / 188,677 / 16,104 / 143,945（21:18） | — | 2,017,038 / 1,321,398 / 348,979 / 189,036 / 16,129 / 144,146（21:35，分别 +4,087 / +2,764 / +737 / +359 / +25 / +201） |

**结论**：断网 4 分钟期间 Zeek 持续产生日志，Filebeat 把发不出去的事件写入各自磁盘队列（峰值约 21 MB，远低于每路 256 MB 上限）；恢复后 61 秒内四路 source topic 被 adapter 全部追平，ES 各域计数相应增长，阻断窗口无数据丢失。队列余量按当前速率可吸收约 50 倍此时长的中断，不触及保留期。"断网不产生重复"这一半由本系列第 1 项（receipt/稳定位置去重）保证，本次证明的是"不丢"与"分钟级补齐"。

**一处口径注意**：spool 段字节是近似值——段文件惰性回收，且 conn/dns/http 在恢复后的残余 spool 含 21:00 整点归档的已知整份重放流量（见第 3 项记录），不是未发送积压；判定追平以 adapter 消费组 lag 为准。

### 2026-09-30 COL-07b 破坏性子集（1/5）：磁盘满触发 ES flood-stage —— 通过

**要验的不变量**：根盘越过 ES flood-stage（80%）时，ES 把全部索引置 `read-only-allow-delete` 保护，索引器写失败**不得提交 offset、不得进 DLQ**；磁盘恢复后块自动解除、积压追平、无丢失。同时验证 capacity guard 的档位上报。

**注入与恢复**（248 本机，受控路径单文件，留 6.4 GB 余量）：

- 注入 A 档：`fallocate -l 5500M /var/tmp/col07b-diskfill.img`（22:50:52，根盘 64%→76%，越过 guard 的 critical 线 75%）
- 注入 B 档：`fallocate -l 9900M /var/tmp/col07b-diskfill.img`（22:52:22，→86%，越过 flood-stage 80%）
- 恢复：`rm /var/tmp/col07b-diskfill.img`（22:55:54，秒级回滚）

**观测**：

| 观测量 | 基线（22:49） | A 档（76%） | B 档（86%，约 3 分钟） | 恢复后（22:58） |
| --- | --- | --- | --- | --- |
| capacity guard 档位 | normal 63% | **critical 1**（75.5%） | **protected 1**（85.5%） | normal 63.4% |
| ES `read_only_allow_delete` | 无 | — | **48/48 索引全部加块** | 删除后 **≤30 秒全部解除** |
| ES cluster health | green | green | green（块是写保护不是故障） | green |
| zeek raw-indexer lag | 0 | — | **734** | **0** |
| 各 standard-indexer lag | 0–5 | — | network 476 / dns 126 / web 91 / tls 2 | 全部 0 |
| raw alias 计数 | 2,035,187 | — | 冻结 | 2,037,205（恢复增长） |

**保护机制行为**：raw-indexer 日志逐条 `raw document write returned 429: cluster_block_exception ... disk usage exceeded flood-stage watermark`；standard-indexer `index N UIM event(s) still failing after 5 attempts`；写失败的组件退出（exit status 1）由 Launcher 按 1→30 秒退避拉起（raw/quarantine/standard 三组 restarts 各 5–8 次），**offset 未提交**，恢复后从原位置重读。**DLQ 三个 topic 的 end offset 全程不动**（9,565 / 10,470 / 509，均为历史旧轮次）——429 被拒写没有进 DLQ，符合"保护性拒绝不是数据缺陷"的设计。22:55:54 删除文件后 2 分 14 秒全部活跃消费组 lag=0，无数据丢失。

### 2026-09-30 COL-07b 破坏性子集（2/5）：Kafka broker 停止与恢复 —— 通过

**要验的不变量**：TUBA 专用 broker（248:29292）停机期间，采集端（21 Filebeat）落磁盘队列不丢数据，消费端崩溃由 Launcher 拉起且不产生重复；broker 恢复后全链路追平。

**注入与恢复**（注入前先查明启动方式）：

- **启动方式结论（新事实）**：TUBA broker 由 `/opt/tuba/collector-live/pipeline-r2/manage_zeek_ingress_broker.py`（start/stop/status，pid 文件 `run/broker.pid`）托管，**没有 systemd 单元**；`is_broker()` 按 `/proc/<pid>/cmdline` 是否含 TUBA 的 `server.properties` 判定，**不会误碰另一产品的 adms broker**（`kafka.service` 管的是 adms 那个，监听 9192，与本测试无关）。
- 注入：`python3 manage_zeek_ingress_broker.py stop`（23:00:12，SIGTERM 优雅停机）
- 恢复：`python3 manage_zeek_ingress_broker.py start`（23:02:10；broker 约 50 秒就绪，脚本随后幂等重建 users/topics/ACLs，每次调用一个 JVM 故整体约 7 分钟才返回——broker 本身早已可用）

**观测**：

| 观测量 | 基线 | 停机约 2 分钟 | 恢复后（23:10） |
| --- | --- | --- | --- |
| 消费端（indexer/normalizer） | running | 连接被拒 `connection refused`，exit 1 → Launcher 退避拉起（restarts 5–10） | 全部 running，消费组 rejoin（broker 日志 GroupCoordinator Stabilized） |
| source-adapter / ingest | running | **存活不退出**：adapter 有界退避重试（`kafka_fetch_retries_total` 增长），ingest 持续监听 | 无重启 |
| 21 四路 Filebeat diskqueue | 近 0 | conn 17 / dns 13 / http 10 / ssl 6 MB | 恢复后回落（conn 10 / dns 4 / http 1 / ssl 6 MB，残余为段文件惰性回收，以 lag 为准） |
| 活跃消费组 lag | ≈0 | 冻结（无 broker 可查） | **全部 0** |
| ES 计数 | 增长 | 冻结 | raw 2,040,613、恢复增长 |

**结论**：broker 停机约 2 分钟无数据丢失；采集端队列吸收、消费端由 Launcher 重建、恢复后约 8 分钟内全部追平。DLQ 无新增。

### 2026-09-30 COL-07b 破坏性子集（3/5）：PostgreSQL 连接中断（仅 TUBA 角色）—— 通过

**要验的不变量**：PG 连接被反复掐断期间，ingest 写 receipt 失败 → adapter **不提交 source offset**、有界重试；恢复后重放被 receipt 去重吸收，不丢不重。共享实例约束：只能用只影响 TUBA 连接的方式，不得停监听、不得动其他角色。

**注入与恢复**：248 本机循环 30 轮（每 2 秒一轮，共 61 秒，23:12:58–23:13:59）执行 `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='tuba' AND pid<>pg_backend_pid()`（每轮实际 terminate 1–2 个正在重建的 TUBA 后端）。恢复即循环自然结束，无任何状态需要回滚。

**观测**：

| 观测量 | 值 |
| --- | --- |
| adapter `delivery_retries_total` | +13（故障期间投递 ingest 失败重试） |
| adapter fetched / accepted | 故障期冻结；恢复后 90,346 → 91,606 追平，**fetched==accepted 恒等** |
| `ingest_receipts` 行数 | 1,683,527（故障前）→ 1,683,970（恢复后，+443，窗口流量全部入账） |
| 消费组 lag / DLQ | 恢复后全部活跃组 lag=0；DLQ 三个 topic end offset 不变 |

**结论**：秒级连接中断对数据面无正确性影响；adapter 的"投递失败不提交 offset"与 receipt 去重是这里的不丢不重保证。

### 2026-09-30 COL-07b 破坏性子集（4/5）：ssl source topic 删除重建 —— 通过，并发现 adapter 对 topic 重建不自愈

**要验的不变量**：低流量 source topic 被删除重建后，采集端积压不丢、ACL 恢复原样、链路恢复消费。

**预置**：导出全部 ACL（`/root/col07b-topic-rebuild/acls-all-before.txt`）与该 topic 的 4 条 ACL；topic 1 分区 RF=1、动态 `retention.ms=43200000`；重建前 adapter 组 lag=0（end offset 145,172）。

**注入与恢复**：`kafka-topics.sh --delete`（23:16:22）→ `--create --partitions 1 --replication-factor 1 --config retention.ms=43200000`（23:16:36）→ `kafka-acls.sh --add` 恢复 4 条 ACL（`tuba-zeek-ssl` WRITE+DESCRIBE、`tuba-zeek-source-adapter` READ+DESCRIBE）。

**三个机制事实（前两个是新发现）**：

1. **KRaft 的 literal ACL 不随 topic 删除而删除**：恢复 ACL 时 4 条均报 `already exists`，与导出逐字一致——ACL 恢复步骤在该版本下是验证而不是重建。
2. **topic 删除会连带删除消费组对该 topic 的已提交 offset**（describe 的 CURRENT-OFFSET 变 `-`）。
3. **缺陷：kafka-go 的消费 reader 在 topic 删除+重建后不自愈。** 其余 5 路 source topic 正常消费，ssl 这路静默卡死：`kafka_fetch_retries_total` 停在 52 不再增长、无任何错误日志、`fetched==committed` 的其他路照常推进，而 ssl context 连续约 13 分钟**零新 raw 文档**（topic 内积压 196 条）。恢复手段是重启组件：`tuba-launcher restart`（23:29:32，manifest-wide，无单组件粒度）后 adapter 因 offset 已被删除而按 `StartOffset: kafka.FirstOffset`（`cmd/tuba-source-adapter/main.go:112`）**从 0 重读**，196/196 全部投递，ssl 组 lag=0，ES tls 恢复增长。**无丢失、无重复**（重读由 receipt/稳定位置去重吸收）。

**遗留**：~~adapter 应对 topic 重建运行期自愈，至少在日志/指标上暴露"该 binding 消费停滞"（当前完全静默，监控只能从侧面发现）；"topic 重建后必须重启消费组件"已写入 RUNBOOK。~~ **已闭环（2026-10-01）**，见下方修复与复验记录。

**修复与复验（2026-10-01，已部署 248 并复现验证通过）**：

- **根因（两层）**：① kafka-go reader 在 topic 删除后 `FetchMessage` 既不返回消息也不返回错误（消费组内部重试吞掉错误），无任何日志/指标，须重启进程；② 在 topic 缺失期间新建 reader 会带着 0 分区分配入组且 topic 重建后不触发 rebalance（复验第一轮实测确认：group 有 member 但 `#PARTITIONS 0`）——因此正确恢复路径是"关闭死 reader → 周期探测元数据 → topic 回归后才建新 reader"，与进程重启语义一致。
- **修复内容**（`internal/sourceadapter/adapter.go`、`cmd/tuba-source-adapter/main.go`）：fetch 改为后台 goroutine + 60 秒停滞 watchdog；`TopicProber` 仅在 broker 明确回答 `UnknownTopicOrPartition` 时判定删除（瞬时故障/空闲 topic 不触发）；判定后关闭死 reader、周期探测，topic 回归后经 `NewConsumer` 工厂建新 reader 按 FirstOffset 从 0 重读；offset commit 改走抓取该消息的同一 reader；停滞首报+每 60 秒周期日志（`consumption stalled: ... waiting Xs`），恢复时记 `consumer replaced ... resuming at FirstOffset` 与 `consumption recovered`；新增指标 `tuba_source_adapter_stall_events_total` / `topic_missing_events_total` / `consumer_replacements_total`。新增测试 `internal/sourceadapter/stall_test.go`（删除→停滞有日志有退避→重建→自动恢复、持续缺失周期日志、错误形态删除、prober 三态），`go build ./...` / `go vet ./...` / `go test ./internal/sourceadapter/` 10/10 通过。
- **部署**：v2 二进制 sha256 `774e054766ecb581ee6d7dccda0f1e834aa61e41338dcd91d4df7c785eb6472c`；旧二进制（`98388bac…`）归档至 `21:/opt/tuba-backup/248/cleanup-20260930/bin/tuba-source-adapter.pre-topic-rebuild-selfheal`；02:48:35 UTC `tuba-launcher restart`（manifest-wide，11/11 running，lag 追平）。
- **复现验证时间线（UTC，全程未重启进程）**：02:49:22 删除 ssl source topic → 02:50:26（+64s）停滞日志出现并关闭死 reader → 02:51:27 周期停滞日志（waiting 1m1s）→ 02:52:11 重建 topic + 核对 4 条 ACL（KRaft 下均 already exists）→ 02:52:27（重建后 16s）探测到回归、自动新建 reader → 02:52:28 恢复消费（FirstOffset）→ 02:54:03 新 consumer-id 提交 offset，lag=0。**对账**：adapter 指标 stall=1 / topic_missing=2 / replacements=1，fetched==accepted==committed 恒等；DLQ log-end 全程 10470 不变；`ingest_receipts` 该 source context 67,648 行 = 67,648 个 DISTINCT `raw_event_id`（重读由 receipt 去重吸收，零重复）；ES tls-zeek 恢复增长（17,408）、集群 green。

### 2026-09-30 COL-07b 破坏性子集（5/5）：积压期间轮换 SCRAM 凭据 —— 通过，并踩出共享身份的连带影响

**要验的不变量**：有积压时轮换组件 Kafka 凭据，组件用新凭据恢复消费、积压追平；轮换过程不得造成认证死锁或数据丢失。

**方法**：`tuba-launcher stop`（23:33:12）制造 160 秒全链路积压 → `kafka-configs.sh --alter --entity-type users --entity-name tuba-zeek-standard-indexer --add-config 'SCRAM-SHA-512=[iterations=4096,password=<新随机口令>]'` → 同步更新 `secrets.json` 的 `services.standard-indexer.password` 与 `/etc/tuba/tuba.env` 的 `TUBA_ZEEK_STANDARD_INDEXER_KAFKA_SASL_PASSWORD` → `tuba-launcher start`（23:35:57）。变更前把 tuba.env 与 secrets.json 备份到 `/root/col07b-cred-rotate/`（回滚=还原文件+重启，秒级）。

**结果与踩到的坑（真发现）**：zeek-standard-indexer 用新凭据认证成功（8 个域组 lag 归 0、restarts=0）。但 **`tenant-a-standard-indexer` 与 `zeek-standard-indexer` 共用同一个 SCRAM 用户 `tuba-zeek-standard-indexer`**（备份中两个 env 变量同值）——只改 zeek 变量导致 tenant_a 全线 `SASL Authentication failed` 退避约 5 分钟。修复：`TUBA_TENANT_A_STANDARD_INDEXER_KAFKA_SASL_PASSWORD` 同步新口令 + launcher restart（23:41:37）。23:44:38 终态：11/11 running、两个 standard-indexer restarts=0、全部活跃组 lag≤3（瞬时在途）、DLQ 三个 topic 全程不变。

**教训**：轮换任何服务身份前必须枚举该 SCRAM 用户的**全部**使用方（同一身份可被多个服务/命名空间共享）；这一条已补入 RUNBOOK「凭据轮转」。

**COL-07b 小结**：六项中五项于 2026-09-30 22:49–23:44 逐项执行完毕（磁盘满 / Kafka 故障 / PG 故障 / Topic 重建 / 积压换凭据），每项均含注入前基线、注入/恢复命令、观测与追平证据；全程无数据丢失、DLQ 零新增、三项保护机制（flood-stage 写保护、Launcher 退避重建、receipt/稳定位置去重）均按设计触发。**目标机重启按用户决定本轮不做**（单节点无带外恢复手段，仍是本项唯一未执行子项）。~~新暴露缺陷一条：source-adapter 对 topic 删除重建不自愈（见 4/5），需修复或固化运维步骤。~~ 该缺陷已于 2026-10-01 修复并在 248 复现验证通过（见 4/5 修复与复验记录），RUNBOOK 已同步更新为新版自愈行为。

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
2. `COL-07`/`V02` 的**非破坏性子集五项全部通过**（重复投递与跨 offset 重发、采集端强杀、日志轮转、归档回放与确认水位、采集端断网后补齐）——已于 2026-09-30 全部完成；
3. `O04` 的目标主机重启恢复验收；
4. `N07`/`I06` 的"暂时重试 vs 永久 DLQ"与容量/过期保护状态的**可查询**验收 —— **已于 2026-10-01 完成**（只读查询，证据分别见 N07、I06 条目内的验收记录）：adapter `/metrics`+日志+消费组 lag+DLQ topic 四路可查重试/永久状态；容量状态查 capacity guard 19100 与 ES 水位读回（75%/78%/80%）；Kafka `retention.ms=43200000` 经 `kafka-configs --describe` 读回生效；永久拒绝的持久聚合在 ES Quarantine（zeek 154,574 条按 stage/code 聚合）。**附带发现两条偏差**：DLQ 保留被 9-30 清理意外从 24h 降为 12h（历史 DLQ 存量已滚空、当前三 topic 均空；**该偏差已于 2026-10-01 修复**，三个 DLQ topic 的 `retention.ms` 恢复为 86400000 并逐条读回确认，见 I06 验收记录第 2 条补记）；`tuba-v1-*` 索引不挂 ILM、ES 保留由 capacity guard 删除实现。两条偏差不改变"可查询"结论。

**不计入 D1 画勾前置**：`COL-07` 的破坏性子集（磁盘满、Kafka/PG 故障、Topic 重建、目标机重启、积压换凭据）——归 D4；其中 6 项之 5 已于 2026-09-30 执行通过（见 COL-07b 记录），仅目标机重启按用户决定暂缓。`V01`–`V10` 的其余项同样归 D4，但其中的非破坏部分可在 D1 期间就做（已在做）。

**D1 画勾（2026-10-01）**：四项证据全部就位，D1（数据底座：单节点可靠原始接入＋DIP/UIM＋八领域检索）按上述完成定义正式关闭。证据指针：① 两条来源链路的逐段计数与确认水位——Zeek 链见 COL-04 条目（四路源 topic 消费组 lag=0、Raw 末位 offset、normalizer/indexer lag、各域 ES 计数）与 COL-07a 5/5 断网补齐的逐段 offset/ES 增量对照表，Windows 链见 T04 后续 2026-10-01 复核（PG `source_instances`→SCRAM/ACL→adapter allowlist→消费组 lag=0→ES raw-tenant_a 按 source_context 分桶计数）与 2026-09-30 复核的 UIM 四域计数（authentication 11,239 / session 8,995 / iam 2,856 / network 12）；两链确认水位即 adapter 消费组 lag=0 与归档确认水位（COL-07a 4/4）。② COL-07a 非破坏性子集 5/5 已于 2026-09-30 完成并勾选。③ O04 目标主机重启恢复于 2026-10-01 两次真实整机重启验收通过（最终 verify PASS=10 WARN=2 FAIL=0），O04 已勾选。④ N07/I06 可查询验收于 2026-10-01 完成（只读，记录分别见 N07、I06 条目）。移出 D1 的 D1.5 项（O03、COL-06、COL-08–15）与 D2 以后各阶段维持原勾选状态，不因本画勾改变。

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

### 2026-10-01 维护窗口：非 root 转换 + 两次整机重启验收（时间线）

| 时刻 (UTC) | 事件 |
| --- | --- |
| 05:44–05:50 | precheck 6/6 PASS；baseline 落盘 `.runtime/o04-reboot/window-20261001`；权限快照+两清单+两 state 归档 `21:/opt/tuba-backup/248/20261001-window/` |
| 05:50 | 放权（不停服务）：`collector-live` 0750 root:tuba、`pipeline` 整树 root:tuba、`/opt/tuba/bin` 组件二进制 root:tuba；`tuba-launcher` 收紧 0750 root:root |
| 05:51 | 候选清单 validate 通过；`stop`（约 10 秒优雅回收 11 服务）→ 换 setpriv 清单 → `start`，数据面主进程换到新映像（`service_control: true`）。**转换停机约 10 秒** |
| 05:52–06:03 | 验收全过（uid=967/NoNewPrivs=1 ×11、端口、lag=0、ES 增长、DLQ 零新增、6/6 UP、tuba 对密钥 DENIED）；稳定 10 分钟 restarts=0 |
| 06:03:56 | 第一次 `systemctl reboot`；关机阶段异常耗时约 34 分钟（原因未查明，journald 非持久；第二次未复现） |
| 06:38 | SSH 恢复。发现 tuba-boot 误判 "already supervised"（status 对陈旧 state 退出 0）+ 29292 broker 无开机入口；手动修 tuba-boot（正证据门槛）、手动起 broker 与 PG（adms daemon 闭锁未自启 PG）、修仓库 verify 解析器同类假阳性 |
| 07:00 | 第三轮 verify：PASS=11 WARN=1 FAIL=0（WARN=api restarts=8，PG 缺位期退避） |
| 07:07:05 | 第二次 `systemctl reboot`；07:08:24 SSH 恢复（约 1.5 分钟）——**tuba-boot 全自动拉起 broker+两清单 16 服务，TUBA 侧零人工**；唯一人工步骤：pg_ctl 拉起共享 PG |
| 07:10–07:20 | 最终 verify：PASS=10 WARN=2 FAIL=0（WARN 为依赖顺序退避的重启计数）；`restart --service zeek-raw-indexer` 实测通过（新 PID、restarts 不增、uid=967） |

**稳定性观察（同每日任务口径，窗口结束时）**：11/11+5/5 running；活跃消费组 lag=0（孤儿 5 个不计）；ES raw-zeek 2,261,057→持续增长、raw-tenant_a 37,888；Prometheus 6/6 UP；根盘 64%。无异常。

**遗留问题**：
1. ~~**Launcher 代码缺陷（待修）**~~（**已修复并部署**，2026-10-01）：`tuba-launcher status` 在 stale state 下现退出非零、输出 `no live supervisor ... (stale state from boot ...)` 且不再打印陈旧 per-service 行（runner_identity 校验 boot_id + pid 启动时刻，覆盖"上一 boot 遗留"与"pid 复用"两种情形）。新二进制 SHA-256 `dc1f3e08…670d0`，旧版已归档 21。tuba-boot 与 verify 脚本的正证据门槛保留为双保险。
2. **共享 PG 无可靠开机自启**：adms daemon 因 STATUS_FILE=1 闭锁不拉起 PG，重启后需人工 `pg_ctl`——TUBA 无人值守恢复受这一外部依赖阻塞，应与对侧产品协商（或接受为已知环境约束）。
3. ~~**首次关机耗时约 34 分钟原因未查明**（journald 无持久日志，第二次未复现）；如需排查应开启 journald 持久化。~~ **journald 持久化已于 2026-10-01 开启**：`/etc/systemd/journald.conf` 设 `Storage=persistent`、`SystemMaxUse=512M`（变更前已备份为 `journald.conf.bak-20261001`），`systemd-tmpfiles --create --prefix /var/log/journal` + `systemctl restart systemd-journald` 后 `/var/log/journal/<machine-id>/system.journal` 已生成并持续写入（disk-usage 8.0M，上限 512M，根盘 64% 无压力）；只重启了 journald 本身，数据面 11/11 running 未受影响。首次关机的 34 分钟原因已无法回溯（当时的日志在 volatile /run 里随重启丢失），此后若复现将有持久日志可查。
4. ~~quarantine/standard indexer 的 `*_METRICS_LISTEN` 在清单有配置但 `ss` 未观察到监听——转换前后一致，与身份无关，待单独核实。~~ **已查明（2026-10-01，只查不改）：不是配置未被消费，也不是监听在未预期地址，而是 248 部署的 `tuba-quarantine-indexer`/`tuba-standard-indexer` 二进制本身没有 metrics server 代码。** 证据：两个二进制内 `strings` 计数 `telemetry` 为 0、不含任何 `*_METRICS_LISTEN` 环境变量名与默认端口字符串（`127.0.0.1:19098`/`19097`）；对照同目录的 raw-indexer/normalizer 二进制含这些符号且实测在监听（zeek 19095/19096、tenant_a 19295/19395）。即这两个二进制编译于遥测接入之前（`cmd/tuba-quarantine-indexer/main.go:39-52`、`cmd/tuba-standard-indexer/main.go:42-68` 的 `telemetry.Serve` 是后加的），属**部署滞后**，非代码缺陷；待修项：下个维护窗口换用当前源码重建的两个二进制（共享 `rawevent` 语义的组件须按 RUNBOOK 同批部署约束评估——这两个组件不是 payload_hash 校验方，不在同批约束内）。**另发现一处清单瑕疵（同源、当时不影响）**：zeek 链的 quarantine/standard-indexer 服务环境变量名是从旧 Python 监督器继承的 `RAW_INDEXER_METRICS_LISTEN`/`SOURCE_ADAPTER_METRICS_LISTEN`（对这两个组件无效），tenant_a 链的 `QUARANTINE_INDEXER_METRICS_LISTEN=19495`/`STANDARD_INDEXER_METRICS_LISTEN=19595` 才是正确名字；待新二进制部署时，zeek 两服务应改用正确变量名并分配不与 19095–19096 冲突的端口。**已修复并部署（2026-10-01 当日窗口）**：从 master（49868ef）构建 linux/amd64 新二进制——quarantine-indexer SHA-256 `78b1422e…af36`、standard-indexer `0d6d60d2…dbda`；旧二进制（sha `238d33c8…e47e`、`c1baf506…93d2`）与旧清单归档于 `21:/opt/tuba-backup/248/20261001-indexer-telemetry/`（归档后 sha256 复核一致）。清单修正：`zeek-quarantine-indexer` 删除继承来的 `RAW_INDEXER_METRICS_LISTEN`/`SOURCE_ADAPTER_METRICS_LISTEN`、新增 `QUARANTINE_INDEXER_METRICS_LISTEN=127.0.0.1:19098`，`zeek-standard-indexer` 同样处理后新增 `STANDARD_INDEXER_METRICS_LISTEN=127.0.0.1:19097`（19097/19098 经 `ss -ltn` 确认空闲；tenant_a 链 19495/19595 不动）；`tuba-launcher validate` 通过（services=11）后换入。部署：`install -m 0750 -o root -g tuba` 替换 `/opt/tuba/collector-live/pipeline/bin/` 两个二进制（sha256 与本地构建一致），`tuba-launcher restart --service zeek-quarantine-indexer --service zeek-standard-indexer` 一次重启两者。注意：运行中的 supervisor 只在启动时读一次清单，`restart --service` 用内存中的旧环境重启子进程——但新二进制只认正确变量名，未设置即取默认值，恰好即 19098/19097，故监听立即生效；清单修正保证下次 supervisor 重启（含 tuba-boot）后语义一致。**验证**：两服务新 PID（28347/28357）、uid=967/gid=965、restarts 计数不增（restart-request 语义正常）；`127.0.0.1:19097`/`19098` 监听、`/health/ready` 均 `ready`；`/metrics` 返回 200 但 body 为空——这是当前代码的真实行为而非部署缺陷：`quarantineindexer.Worker`/`standardindexer.Worker` 根本没有接收 metrics registry（对照 `rawindexer.Worker` 有 `Metrics` 字段、19095 有 `tuba_raw_indexer_indexed_total`），而计数器只在首次 Inc 时注册，故这两个组件的 `/metrics` 恒为空 200，健康信号以 `/health/ready` 为准；若需要计数须另行接线（属代码改动，未在本次范围内）。消费组 lag=0 且有活跃 member（quarantine 163703/163703、standard network 1519334/1519334、dns 402576/402576）；DLQ end offset 9565 零新增；ES quarantine-zeek +10/30s、network-zeek +121/30s 恢复增长；其余 9 服务 pid/restarts 未变，Prometheus targets 不变。回滚路径：还原 21 归档的两个二进制与清单后再次 `restart --service`。
