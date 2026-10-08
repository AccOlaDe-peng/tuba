# TUBA 产品详细设计基线

> 2026-10-08 身份边界修正：TUBA 使用内置系统账号与服务器会话；247 Keycloak 仅属于外部 Linux 认证/未来日志来源。历史 OIDC 登录记录已被 [系统登录](SYSTEM-LOGIN.md) 替代。

版本：1.0｜日期：2026-09-29｜状态：设计定版，待实现与验收

本文补齐完整单节点产品范围内尚未明确的详细设计。总体边界以
[目标架构](TARGET-ARCHITECTURE.md) 为准，实施状态以
[实施 TODO](IMPLEMENTATION-TODO.md) 为准。本文中的“定版”表示实现团队不再需要业务设计选择，
不表示对应代码、部署或验收已经完成。

发生冲突时，顺序为：本文和目标架构中的较新明确决策、机器可读合同、实施 TODO、专题文档、历史验收记录。
P2 多节点扩展不属于本轮设计缺口。

## 1. 全局非功能边界

- 当前交付形态是单节点、单实例组件，由 `tuba-launcher` 或 TUBA Management Agent 调用 Launcher 管理；不注册
  systemd 或 Windows Service。主机启动后的恢复由受控开机任务调用 Launcher，任务本身只保存启动命令，不保存密钥。
- 所有内部队列均有界。达到磁盘 80% 或对应队列硬上限时停止接受新增写入并告警，不通过删除未确认数据恢复服务。
- 默认语义为至少一次投递、稳定业务 ID 幂等落地。只有 normalizer 的 Kafka 输入/输出链使用事务和
  `read_committed`；PG 与 Kafka 之间使用 inbox/checkpoint/outbox 协议，不宣称分布式恰好一次。
- 开发环境允许 loopback HTTP。生产中浏览器/API、Agent 管理通道及跨主机数据通道必须使用 TLS 1.2 以上；
  PG 使用 TLS，Kafka 使用 SASL_SSL，ES 使用 HTTPS。证书和私钥由受限文件或环境引用注入，不进入发布包、命令行或日志。
- `tenant_id`、namespace、generation、release、route 和权限都来自服务端登记或已签名发布包；任何事件正文中的同名字段均不可信。
- 时间统一存 UTC，API 使用 RFC 3339，稳定日期索引由合同指定的事件时间或采集时间生成，禁止以重试时刻改变目标索引。

## 2. 来源、采集与接入治理

### 2.1 来源生命周期

来源状态固定为 `draft → staged → active → draining → retired`。只有 `active` 可获得写 ACL；一个来源实例同一时刻只能有
一个 active context。发布新 context 时先创建新 Topic、凭据和绑定，再切换 Agent 配置；旧 context 进入 `draining`，
待 Beat 队列为空、adapter offset 追平且 Raw receipt 水位对账后撤销写 ACL。`retired` 不可重新激活。

Topic 重建不得复用 context、流实例 ID 或来源凭据。凭据轮换采用最长 24 小时重叠窗口；新旧凭据均只允许写同一精确 Topic，
确认新凭据生效后撤销旧凭据。禁用来源立即撤销写 ACL，但保留状态和审计。

### 2.2 Windows Security 首期范围

139 和 169 使用 Winlogbeat 8.19.x 独立实例，只读取 `Security` channel，启用原生 XML，并以
`computer/channel/record_id/event_time` 构成稳定位置。服务账号只授予读取 Security 日志所需权限，不加入本地管理员组。

首期事件集固定为：

- 登录与会话：4624、4625、4634、4647、4648、4672、4778、4779；
- Kerberos/NTLM：4768、4769、4771、4776；
- 账号：4720、4722、4723、4724、4725、4726、4738、4740、4767；
- 组成员关系：4728、4729、4732、4733、4756、4757；
- 目录与策略：4719、5136、5137、5141；
- 进程证据：4688，仅在主机已配置命令行审计时采集；缺命令行不降低认证事件质量。

4719 明确定义为“系统审核策略变更”，进入 `iam` 领域并保留变更前后策略值，不作为登录事件。其他 Event ID 默认不采集；
扩展范围必须形成新接入包版本并重新做容量测量。日志清空、RecordID 回退、bookmark 不可读和来源时间倒退均产生缺口事件，
不能静默生成新位置。

### 2.3 Zeek 与其他来源

Zeek 首期只接入 `conn/dns/http/ssl` JSONL。Filebeat registry 是读取水位权威，归档文件只有在 registry 证明 EOF、Kafka 已确认且
adapter/Raw 水位对账后才能回收。文件轮转以 device/inode/offset 识别，不以路径或正文 hash 识别。

Syslog 网关首选 TCP+TLS，按设备绑定 tenant/source，落入持久文件后由 Filebeat 发送；UDP 只提供尽力交付并显式标记
`evidence.delivery=best_effort`。JumpServer 先使用审计文件或 Syslog，API 仅补足无法从日志取得的资产引用；录像和文件只保存受控引用。
系统登录成功/失败、锁定、退出、改密和创建账号由 Go API 写追加式审计；服务运行日志不作为完整审计来源。247 Linux 认证日志接入另行实施，当前不属于系统登录。

专用连接器统一采用：分页游标持久化、可配置重叠窗口、供应商限流退避、Webhook 签名验证后持久确认、数据库按稳定复合键增量。
连接器必须先在一个真实来源通过重启、重复页、限流和游标损坏验收，才能复制到其他来源。

来源发布状态 `draft/staged/active/draining/retired` 描述 context 的版本和切换；接入许可状态
`active/paused/revoked` 描述该 context 当前能否继续接纳消息。两者是不同维度，不能写入同一枚举字段。
只有发布为 `active` 且许可为 `active` 才能接纳。`paused` 保留读取水位与重试；`revoked` 需先持久化
拒绝证据再推进 adapter offset。`draining` 只允许处理切换前已确认边界内的积压，边界外拒绝；
`retired` 不接受新消息。控制面必须在一次事务中记录切换边界、状态变更和审计，再由 outbox
执行 ACL 变更；ACL 实际状态与期望状态不一致时停止发布完成判定。

### 2.4 过滤与永久拒绝

过滤策略是不可变版本，包含来源范围、规则、原因码、预计影响和回滚目标。新策略先以 `shadow` 运行至少 24 小时，记录匹配数、
按 Event ID/dataset 的分布和受保护场景命中数；审批后转 `enforced`。认证失败、管理员操作、账号/组变更、审计策略变更和缺口事件
是保护类别，不能仅靠源端复杂表达式过滤。源端过滤与 adapter 准入过滤分别计数，平台不得把 adapter 过滤描述为节省网络流量。

永久拒绝只包括：超过硬大小限制、无法解析为单条 Beat 对象、缺少稳定来源位置、来源类型与登记不符、合同版本不支持。
**暂时性**失败——凭据识别不出、映射缺失、PG/Kafka/ingest 不可用、限流——均为可重试错误。**确定性拒绝**不是：平台已认出该来源并判定不予接纳时（来源被撤销），重试不会改变答案，而扣住 offset 只会把记录交给 topic 保留期、且不留下任何被拒的证据；这一类必须以 **403** 表达，由调用方隔离后前进。来源的三种生命周期状态 `active`/`paused`/`revoked` 即为此区分而设：`paused` 是运维打算恢复的暂停，保留 offset；`revoked` 是退役，写入 DLQ 并提交 offset。永久拒绝必须先写持久 DLQ，DLQ 失败时不提交输入 offset——这条对 `revoked` 同样成立。

## 3. UIM、质量与隔离

质量状态只有 `qualified`、`partial`、`quarantined`。`partial` 表示仍满足领域最小语义，可索引和查询，但只能进入显式允许 partial
的特征；`quarantined` 不进入领域 Topic。Raw 始终保留原文引用。

首期边界定为：

| 数据 | qualified | partial | quarantined |
| --- | --- | --- | --- |
| Zeek TLS | 有连接端点及版本/握手结果；SNI 有则保留 | 缺 SNI，但端点和握手语义完整 | 缺端点、时间或无法识别握手结果 |
| Zeek DNS | 有 query、qtype、端点和时间 | 有 query 但 answer/rcode 缺失 | 缺 query、端点或时间 |
| Zeek HTTP | 有方法、端点、时间，且有 host 或可解析的绝对 URI | 缺 host，但 URI/IP 仍可确定目标；或缺 status | 缺方法、目标端点或时间 |
| Zeek conn | 有双端点、transport、时间 | 缺计数器或状态 | 缺端点、transport 或时间 |
| Windows 登录 | 有 computer、Event ID、RecordID、时间和主体 SID/名称之一 | 目标账号或网络字段缺失 | 缺稳定位置、主机、事件时间或主体 |

未知枚举保留原值并映射为 `unknown`，不虚构语义。类型错误、解析错误、必需字段缺失、规则异常分别使用稳定原因码。
Quarantine 保存 Raw 引用、阶段、release、规则 ID/版本、失败字段、原因码、首次/最近失败时间和修复状态。修复通过受控回放产生新
generation；不得原地修改隔离证据。

## 4. 任务、事务与发布

PG 是任务状态、租约、业务 checkpoint 和 outbox 的权威。worker 用 `FOR UPDATE SKIP LOCKED` 领取任务；lease 包含 owner、epoch、
expires_at。每次写入都校验 epoch，旧租约写入返回 fencing 冲突。任务状态固定为
`queued/running/succeeded/failed/cancelling/cancelled`，取消只在安全 checkpoint 生效。

单条或有界批次事务内完成 inbox 去重、业务状态、checkpoint 和 outbox 写入。outbox publisher 只发送已提交记录，按聚合键保持顺序，
Kafka ACK 后标记 sent；崩溃重发依赖稳定 message ID 去重。checkpoint 超过 Kafka retention 时进入 `gap_detected`，禁止自动跳到 latest。

发布包状态固定为 `draft → validated → staged → active → retired`。校验覆盖签名、哈希、合同兼容、依赖图、Mapping 和资源预算。
每类资产只能有一个 active 版本；激活由 `publisher` 权限执行，带幂等键和审计。资产不可原位覆盖。回滚是重新激活兼容的历史版本，
必要时创建新 generation，不修改历史结果。

## 5. 实体与归因

首期实体只有 `account` 和 `device`，ID 包含 tenant、identity namespace、规范化标识及版本。强标识为 SID、目录 object GUID、设备 UUID、
稳定 agent ID；弱标识为用户名、邮箱、hostname、IP。弱标识只能在明确时间和来源范围内关联，不能跨 tenant 或身份空间自动合并。

规范化规则：SID/GUID 小写并校验格式；UPN/邮箱按域大小写规则规范化，原值保留；Windows `DOMAIN\\user` 分拆 namespace/name；
hostname 小写、去尾点但不擅自补域；IP 使用标准文本形式。冲突时保持多个候选并标记 `ambiguous`，禁止“最近一个”覆盖。

每个事件的角色独立归因，如 actor、target、source_device、destination_device。结果为 `resolved/unresolved/ambiguous`，包含证据、规则版本、
有效时间和置信级别；不得改写 event ID。关系使用 `[valid_from, valid_to)`，新事实不能覆盖历史区间。实体投影采用单调 revision，旧 revision
不得覆盖新值。

## 6. 特征、基线、检测与风险

正式调度入口只有 `tuba-analysis-worker`；Go 认证检测 CLI 仅用于诊断。registry 中每个特征声明领域、角色、字段、质量门槛、窗口、
allowed lateness、idle timeout、去重键和版本。默认 allowed lateness 为 10 分钟，partition 5 分钟无输入后参与 idle watermark；事件时间
超过接收时间 5 分钟的记录进入 `future_time` 隔离。状态最多保留 `lookback + lateness + 1 window`。

首期特征固定为：账户登录尝试数、失败数、失败率、来源设备数、来源 IP 数，以及失败后成功序列。基线按 tenant/entity/feature/version/
generation 训练，最少 14 个完整日且至少 100 个样本；不足时为 `cold_start`，只运行明确允许冷启动的确定性规则。模型发布包含训练截止时间、
样本范围、评估指标和不可变版本。

14 日训练输入来自按窗口持久化的**特征样本**，不依赖 Raw、标准事件或 Kafka 保留 14 日。
样本至少保留 `训练观察期 + 允许迟到/修订期 + 回滚窗口`，实际期限由容量预检批准；不足则保持
`cold_start`，不得从已过期的事件索引假装补齐。训练只读取截止水位前已关闭、质量合格、
同一 feature version/generation 的窗口；修订或撤回后重训产生新不可变模型版本，旧模型保留审计引用。
若特征样本预算不能满足 14 日，应在发布前阻断统计基线场景，确定性规则仍可运行。

首期检测包括：短窗失败聚集、失败后成功、相对历史基线异常。结果必须包含解释、阈值、贡献特征、模型/规则版本和输入引用。
同一业务键修正时 revision 递增；结果失效发布 `retracted`，不删除历史。迟到但仍在保留边界内的数据重算相同窗口；超界数据只能通过
受控回填进入新 generation。

风险由不可变贡献组成。异常新增/修订/撤回分别产生正贡献、差额补偿或反向补偿；当前风险是未过期贡献的确定性聚合。
默认半衰期 7 日、贡献 30 日后归零，规则可声明更短周期。风险投影记录计算版本、更新时间和贡献引用。自动建案默认关闭；启用时使用
`tenant/entity/policy/time_bucket` 去重键。反馈只进入审计与离线评估，不在线修改模型。

## 7. 查询、导出与产品界面

Data Model Catalog 为每个 dataset 声明字段、类型、敏感级别、可搜索/聚合能力、质量要求和 active generation。查询必须固定 tenant、namespace、
dataset、generation 和时间范围。默认范围 24 小时，最大 31 日；默认 100 条、单页最多 1000 条；聚合最多 10,000 buckets；同步查询 30 秒超时。

31 日是 API 的绝对请求上限，不是数据可用性承诺。实际可查询窗口取请求范围、该 dataset
当前批准保留期及现存分区的交集；超出保留边界返回明确的 `retention_exceeded`，不得静默截短、
返回空结果冒充无事件，或跨代次混查。UI 展示各 dataset 的实际可查询起点和索引新鲜度。

SPL 只支持 `search` 的等值/比较/布尔条件、字段选择、排序、`stats count/sum/min/max/avg by`、`timechart` 和有界 `top/rare`。
字段必须来自 Catalog；禁止子查询、join、eval、正则回溯表达式、任意 ES DSL、脚本字段和未限界查询。逻辑计划在执行前计算资源预算，
超限返回稳定错误码。

导出始终异步，固定查询快照、字段白名单和上限；默认 100,000 行或 1 GiB，先到者停止。下载时重新授权，链接 15 分钟过期且单次使用。
原文和敏感字段分别要求 `raw:read`、`sensitive:read`，创建、完成、下载、过期和失败均审计。

控制台最终导航固定为：总览、事件、异常、风险、实体、案件、来源、数据质量、发布与任务、回放、审计、运行与备份、访问控制。
每个列表使用游标分页；每个详情显示血缘、版本、质量、索引新鲜度和权限拒绝状态。演示数据只允许显式 demo namespace，生产构建默认关闭。
前端权限只改善交互，API 始终重新授权。

## 8. 部署、安全、观测与恢复

正式网络分为用户入口、管理面和数据面。反向代理只暴露用户 API/Web；ingest、metrics、PG、ES 和内部 worker 端口保持 loopback 或受限数据网。
Management Agent 使用短期注册 token 换取独立身份，后续采用双向 TLS；证书轮换重叠 24 小时，禁用 Agent 立即撤销证书和来源写 ACL。

组件升级包必须有 SHA-256、签名、OS/架构、合同兼容范围和状态格式版本。Agent 下载到临时目录，验签后停止组件、原子切换 `current`、
通过 readiness 观察 5 分钟；失败则回切上一版本。registry/data 目录不随二进制回滚。一个主机一个 Agent 单实例锁，每组件独立 data/registry。

外部备份目标必须是不同故障域的 S3 兼容对象存储或受控备份主机，不允许把同盘目录称为备份。PG 使用每日 base backup + 连续 WAL，
设计目标 RPO 15 分钟/RTO 2 小时；ES 每日 snapshot，RPO 24 小时/RTO 4 小时；系统账号摘要和会话随 TUBA PostgreSQL 备份；发布包、配置和审计清单每日备份。
Kafka RF=1 不作为备份，恢复依赖 PG checkpoint、Raw/ES snapshot 和仍在 retention 内的 Topic。未配置异机目标前，系统必须显示
`backup_not_configured`，不得承诺磁盘或整机损失 RPO/RTO。

所有服务提供 live/ready/metrics。告警至少覆盖磁盘水位、依赖不可用、lag、新鲜度、DLQ、备份失败、证书/凭据临近过期和 Agent 离线。
邮件/receiver 是部署参数，不是设计选择；未配置时 UI 明确显示“仅本地告警”。日志按大小轮转、总量有界，禁止记录 secret 和原始敏感正文。

## 9. 回放、代次与验收判定

回放任务固定来源/时间/release/generation/input watermark，默认写影子 generation。追平条件同时满足：输入 checkpoint 到达固定尾水位、outbox=0、
下游 lag=0、索引新鲜度通过且逐段计数对账。激活通过 PG 原子更新 active generation 指针；查询切换后保留旧 generation 直到回滚窗口结束。
回滚同样切换指针，新旧 generation 不得同时进入风险聚合。

V01–V10 使用机器可读样例、故障注入记录、逐段水位/数量、权限拒绝和恢复报告验收。设计文档、单元测试、进程存活或一次成功演示均不能单独关闭验收项。
每个 TODO 关闭证据必须包含版本/提交、配置或合同路径、执行环境、输入范围、实际结果、遗留限制和责任人。

## 10. 设计关闭映射

本文关闭以下“需要产品设计选择”的问题，但不关闭其实现任务：

- I01–I06、COL-01–COL-15：来源生命周期、Windows 范围、确认、过滤、Agent、升级和迁移语义；
- N01–N08：事务边界、质量规则、Quarantine、DLQ 和 generation；
- T01–T05：任务状态机、租约、inbox/outbox、发布和 retention；
- E01–E05：实体空间、规范化、归因、时态关系和投影；
- F01–F08、R01–R04：窗口、水位、冷启动、检测修订、风险补偿和反馈；
- Q01–Q03、W01–W05：Catalog、SPL 子集、导出和最终信息架构；
- B01–B06、V01–V10：回放切换、升级、异机备份、缺口处理、Runbook 和验收证据格式。

部署地址、真实凭据、备份接收端、证书 CA、告警 receiver 和业务责任人属于环境配置，不属于未完成设计；缺失时按 fail-closed 状态呈现。
