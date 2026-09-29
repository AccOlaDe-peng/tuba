# TUBA 采集体系设计

版本：2.1｜日期：2026-09-29｜状态：设计定版，待实现与验收

本方案替代从零开发文件与 Windows Event Log 采集器的路线。Collector 是 TUBA 管理下的采集体系总称，不再表示一个自研程序读取所有来源。现有代码尚未完成此替换。任务以 [实施 TODO](IMPLEMENTATION-TODO.md) 的 COL-01–COL-15 为准；[旧设计及验收记录](history/COLLECTOR-DESIGN-20260926.md) 保留作迁移依据。

来源生命周期、Windows Event ID、质量、过滤、Agent 安全、升级和恢复的最终细则见 [产品详细设计基线](DESIGN-BASELINE.md)。

数据流图：[采集与索引数据流](diagrams/collector-v4.html)。图通过 showcase 结构校验及 1440×900、1600×1000、1920×1080、2048×1320 自动浏览器视口检查；旧版 [collector.html](diagrams/collector.html) 只作历史记录。

## 1. 定版决策

- Filebeat 负责文件日志；Winlogbeat 负责 Windows Event Log；成熟 Syslog 网关负责设备日志接收与落盘；专用连接器处理 API、Webhook、数据库或私有协议。
- TUBA Management Agent 使用 Go，实现注册、配置生成、子进程控制、健康上报及升级；不再开发通用文件读取器或 Windows 原生事件读取器。
- 包含多个可执行文件但使用统一目录布局和 CLI。Linux/Windows 按 OS/架构选择锁定版本组件，不注册 systemd 或 Windows Service。操作系统重启后的自启动方式独立配置，不能把无需服务安装解释为自动开机恢复。
- 首期每功能一个实例。平台继续使用单 Kafka broker、单 PG、单 ES，不部署 Fleet、Elastic Agent 或来源端 Kafka。
- 开发阶段管理 API 和现有 ingest 使用 HTTP；Kafka 使用受限网络及独立 SASL 凭据。生产传输保护单列 O03，不阻塞当前开发。

## 2. 数据流与部署边界

```text
文件 / Zeek / 应用事件文件 → Filebeat ──────────┐
Windows Event Log          → Winlogbeat ─────────┤
设备 Syslog → 网关持久文件 → Filebeat ──────────┤
厂商 API / Webhook / DB    → TUBA 专用连接器 ───┤
                                                ↓
                   Kafka 来源接入 Topic（非可信 Raw）
                                                ↓
                     tuba-source-adapter
                                                ↓
                    tuba-ingest HTTP + receipt
                                                ↓
                         Kafka 可信 Raw
                            ├→ Raw Indexer
                            └→ DIP / UIM → 标准索引与分析

TUBA API / PG ← HTTP 心跳与配置轮询 ← Management Agent
                                       └→ 管理 Beat / 网关 / 连接器
```

实现状态：`cmd/tuba-source-adapter` 与 `internal/sourceadapter` 已有首个单进程消费骨架。adapter 本地配置只列允许消费的 Topic，不包含来源 context/API key；它使用 `SOURCE_ADAPTER_TOKEN` 调用内部 Beat ingest 路由，由服务端依据 `tuba.source.<context_id>.v1` Topic 查询启用的来源实例和不可变 context。adapter 仅在 202 receipt 的服务端 context、`topic/partition/offset` 位置和原文 SHA-256 匹配后提交 offset；本地识别的无效 Beat 事件先写持久 DLQ 再提交，所有 HTTP 错误（包括授权/配置错误）都保留 offset 并退避重试。`SOURCE_ADAPTER_METRICS_LISTEN` 提供独立 Prometheus 文本端点，默认只监听 `127.0.0.1:9185`，记录读取、接收、重试、DLQ 和 offset 提交结果。配置示例见 `deploy/components/source-adapter.example.json`。Management Agent 配置下发、Kafka ACL 编排、token 安装和轮换、指标 dashboard/告警、崩溃/重复消息验收及远程运行仍未完成；这只是开发骨架，不代表生产闭环。

首批选择 Beat 原生 Kafka 输出，避免自研 Beats 协议服务端或把标准 HTTP 输出误当 TUBA receipt 协议。Broker 需要能被来源主机通过配置的 advertised listener 访问；仅暴露受限数据监听端口，不向来源开放 controller/PG/ES。不能到达 broker 的网络区域暂不启用该拓扑，后续网关接入需独立设计。

接入 Topic 按来源上下文分配，命名 `tuba.source.<source_context_id>.v1`，隔离验收可加固定 profile 前缀；默认单分区、RF=1。每个来源上下文使用独立写入凭据和精确 Topic ACL，禁止自动创建 Topic、跨来源写入及直接写 Raw。共享主机凭据只隔离主机，达不到此处来源隔离要求，因此首批按来源上下文拆分 Beat 实例、凭据和 data 目录。

Kafka 消费者不会收到可信生产者 principal。adapter 必须用服务端登记的 Topic→organization/source/context/release 映射绑定身份，忽略正文中的租户、路由、版本声明。Topic 创建、ACL 和绑定成功后才允许下发来源配置；绑定不可原位更改。管理通道仍采用 HTTP，不使用 Kafka 管理 Topic。

## 3. 确认、游标、ID 与证据

| 边界 | 成功含义 | 未成功时 |
| --- | --- | --- |
| Beat → 接入 Kafka | broker 确认接入消息；配置 acks=all | Beat 重试并使用已验证的磁盘队列/registry；不能承诺所有故障均无损 |
| adapter → ingest | 匹配的 202 receipt，可信 Raw 已由 Kafka 确认 | 保留接入 offset，固定正文、位置和上下文重试 |
| 标准/Raw indexer → ES | ES 成功或经过等价内容核验 | 重试或持久 DLQ 后推进对应消费位点 |

adapter 在 receipt 确认后提交接入 offset；崩溃重读通过 receipt 去重。永久拒绝写入独立接入 DLQ，确认后才可推进 offset；临时错误退避，不能无限跳过或将 4xx 一律吞掉。大消息以“Beat 外层包装＋原文＋Raw 信封”的实际字节预算约束，各层大小必须一致，不能无提示截断。

两种标识分开记录：

- 来源事件位置：文件身份/代次＋字节位置，或原 Windows 主机/频道/代次/RecordID；用于识别同一真实记录。不能只用文件路径、正文 hash 或 RecordID。
- 投递位置：接入流实例 ID＋Topic＋partition＋offset。Topic 重建必须创建新流实例和上下文。它可保证 adapter 重读同一 Kafka 消息的幂等，不保证 Beat 重发到不同 offset 时去重。

2026-09-29 的 21 隔离 Kafka 故障实测证明：Filebeat 8.19.0 重启重放会产生 at-least-once duplicates；重复 Beat 记录的 `log.file.device_id`、`log.file.inode`、`log.offset` 和消息正文稳定。现合同定义 Filebeat 行位置为 `filebeat-v1:<device_id>:<inode>:<log.offset>`；Windows 位置为 `winlogbeat-v1:<hex(computer_name)>:<hex(channel)>:<record_id>:<UTC timestamp>`，避免 RecordID 在日志清空后重用导致身份冲突。Raw ID 依据稳定源位置；Kafka 的 `kafka-v1:<topic>:<partition>:<offset>` 单独放入 `delivery_position`。Zeek 缺少文件坐标、Windows 缺 computer/channel/RecordID/time 时接入拒绝并保留 Kafka offset。adapter 对 receipt 校验当前投递位置、context 与 payload hash；同一源事件跨 offset 重发复用原 receipt。新版本已部署到 248 的 ingest、adapter、Raw indexer、normalizer，原 consumer group 与 offsets 保留。由于当前旧 Raw ES 日索引的 `dynamic:strict` mapping 不包含 `delivery_position`，完整投递坐标保存在 PostgreSQL receipt 与 Raw Kafka，Raw ES serializer 省略这个可选字段，无需管理员原地修改历史 mapping。单元和全量 Go 测试通过；仍需在隔离主题完成真实跨 offset source→receipt→Raw→UIM 唯一性对账，完成前不关闭 COL-03/COL-07。

Filebeat 保留原 message、编码、多行边界与来源元数据；Winlogbeat 启用并验证原生 XML 保留。原生证据与 Beat 规范化字段分别保存，不能把 ECS 文档冒充原始字节。无法取得原生证据的 API 来源记录其证据等级。DIP/UIM 仍由 TUBA 执行，不依赖 Elastic ingest pipeline 自动在 Kafka 路径生效。

## 4. 来源接入包

| 来源 | 采集方案 | 接入包必须声明 |
| --- | --- | --- |
| Zeek 21 | 新的 TUBA 专用 Filebeat 实例采 conn/dns/http/ssl JSONL | 路径、轮转方式、dataset、字段样例、位置与原文映射 |
| Windows 139/169 | 每台主机独立 Winlogbeat Security channel、独立 registry/queue/context/凭据 | 24 个 Event ID 白名单、XML、computer/channel/RecordID/time、log-clear 1102 与日志覆盖缺口 |

Zeek 开发配置模板为 `deploy/components/zeek-filebeat.example.yml`：conn/dns/http/ssl 各运行一个独立 Filebeat 进程，分别使用 source context/Topic、精确 Kafka 写入凭据及 data/registry 目录。2026-09-27 在 21 用独立管理器 `scripts/manage_zeek_filebeat.py` 部署验证；活动日志从 `/opt/zeek/spool/zeek/<dataset>.log` 读取，最近 90 分钟的每小时 gzip 归档由同一管理器启动的 `archive-sync` 进程解压到 TUBA 私有 spool，再由 Filebeat 读取。归档先写临时文件，gzip 校验成功后原子改名，Filebeat 不会读到半个归档。该步骤是必要的：21 的 Filebeat 8.19.0 filestream 不解压 gzip，最初误将压缩字节作为日志发送，DIP 隔离了这些记录；修正后抽样确认 source Topic 中的归档行是有效 JSON，`event.original` 与 `message` 一致。管理器不调用 systemd；原有 systemd Filebeat 配置、registry 和进程保持不动。原始 Beat 包装与 JSON 行共同进入 Raw；Zeek DIP 校验可信 `event.dataset` 与原始事件。

真实闭环快照：21 的四路独立 Filebeat → 248 隔离 SCRAM/ACL Kafka → source-adapter receipt/offset commit → Raw → DIP/UIM → 四个 ES domain alias 均有数据。修正后的归档消息继续由 Raw 保留；首轮压缩字节仍保留在 Raw 与 quarantine 作为故障证据。无 SNI 的 TLS、缺 query 的 DNS、缺 host 的 HTTP 依据现行 UIM 合同进入 quarantine。A03 已将每路 disk queue 定为 256 MB、解压 stage 定为 6h、Kafka 定为 24h、ES 定为 7 日；stage 过期清理已进入管理脚本，部署与故障演练归 COL-07/O05。

Filebeat 的 `message_max_bytes` 会截断超过上限的行，因此模板没有设置较低的自定义值。默认读取上限、Kafka `max_message_bytes`、broker 消息上限与 adapter 的 1 MiB Raw 合同仍需统一预算并用超大行验证；当前模板不能承诺所有超大行均完整进入接入 DLQ。

Windows 模板见 `deploy/components/windows-security-winlogbeat.example.yml`，Event ID 为 4624、4625、4634、4647、4648、4672、4719、4720、4722、4723、4724、4725、4726、4728、4729、4732、4733、4756、4757、4768、4769、4771、4776、1102。8.19.0 官方包已核验；139/169 的临时 24h shadow 读取均配置 `include_xml:true` 并成功产出带 XML 事件，结果只输出按 ID 的数量。当前 scope 实测 139 为 1,762 条（4719=1,728、4776=34），169 为 111 条（4776=111）。尚未启动正式 Kafka 输出：需要通过有权 `source:manage` 的 API 操作者建立每台来源登记/context/release，随后创建独立 SCRAM 写入用户和 exact Topic ACL，并更新 adapter topic allowlist。不能复用 Zeek 用户或绕过 API 身份审计。
| Windows Security | Winlogbeat 本地读取；按范围配置 Event ID | 权限、频道、XML、bookmark、清空/覆盖缺口；WEF 必须保留原发出主机 |
| Syslog | rsyslog/syslog-ng 网关持久落盘后 Filebeat 读取 | RFC3164/5424、framing、时区、多行、大小与设备绑定；UDP 为尽力交付，不承诺无损 |
| JumpServer | 文件/Syslog 采集运行事件；审计 API 用连接器 | 实际版本、登录/资产访问/命令审计覆盖；录像和文件证据保存受控引用 |
| Keycloak | 开启用户事件/管理员事件及合适 listener 后采集文件；必要时 API 连接器 | 实际版本、事件开关、事件种类、脱敏；默认服务日志不代表完整审计 |
| SaaS / API / DB / Webhook | 版本化专用连接器 | 分页、限流、增量游标、重叠补采、去重、签名认证与证据保留 |

21 上既有 Filebeat 属于其他链路，与 TUBA 无关。新实例使用独立配置、data/registry、日志、凭据和输出，不能接管既有实例。

## 5. 过滤与容量

先配置必要的来源范围，再启用基于真实场景的过滤。每次变更固定 policy_id/version，保留应用审计。原始证据保留仅覆盖实际接收事件，源端丢弃的数据无法在平台回放。

Beat 原生过滤用于范围选择和经过批准的简单降噪；逐规则影子计数、丢弃原因和保护场景检查不是 Beat 默认具备的能力。TUBA adapter 先实现影子统计和平台准入过滤；这会减少 Raw/ES 写入，但不会减少来源到接入 Kafka 的流量。源端复杂过滤须在实现可审计计数后才启用；不能把平台端过滤宣称为端侧流量削减。

预算同时包含 Beat 队列、Syslog 文件、接入 Kafka、Raw Kafka、PG receipt 和 ES；设高低水位、过期预警及源端覆盖告警。接入 Topic retention 必须覆盖允许故障与追赶窗口，超过窗口生成缺口任务，禁止静默跳到 latest。两个 Kafka 阶段共享一个 broker，不具备异机副本保护。

## 6. 包、升级与迁移

统一布局：`bin/` 管理代理，`components/<name>/<version>/<os-arch>/` 第三方组件，`config/releases/` 生成配置，`data/<source_context_id>/` 独立状态，`logs/` 轮转日志，`manifests/` 版本/哈希/许可清单。组件版本先核实可用制品、Kafka 兼容及再分发条件，默认支持从批准地址下载并校验，不预设可任意重新分发厂商二进制。

管理代理保持现有 CLI 命令习惯；子进程从允许清单启动，以参数数组调用，不执行平台下发 shell。Beat 升级要验证 registry 格式兼容；不兼容时不能直接降级打开已升级状态目录。保留可恢复检查点、处理在途消息和健康确认后才切换。

| 现有资产 | 处理 |
| --- | --- |
| enrollment、心跳、ETag、租户/RBAC、source context、receipt | 复用并扩展能力/组件状态、Topic 绑定及配置 schema |
| CLI/进程管理与包脚本 | 改造成 Management Agent 和组件监督器 |
| 自研 Zeek reader、通用 SQLite spool、旧逐条 Sender | 冻结新增需求，保留迁移排空工具和历史测试；新链路接管后再退役 |
| 自研 Windows adapter 计划 | 取消，改为 Winlogbeat 接入包 |
| DIP/UIM、Raw 索引和标准索引 | 保留，增加 Beat 包装字段到厂商原文的适配 |

迁移顺序：锁定版本与合同→独立 namespace/Topic 实验→Zeek 验证→Winlogbeat 验证→Syslog→JumpServer/Keycloak 接入包→管理和升级闭环。旧队列先排空并记录水位；旧新路径不同时写同一生产 generation。切换有来源边界与回滚水位，不能仅换二进制。历史 16 条 Zeek 演练只证明旧自研路径，不算新方案验收。
