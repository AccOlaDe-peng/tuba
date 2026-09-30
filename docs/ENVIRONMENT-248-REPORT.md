# 248 环境只读盘点

采集时间：2026-09-27 08:32–08:45（Asia/Shanghai）  
目标：`10.6.68.248`（主机名 `centos-128`）  
范围：资源、监听端口、服务/进程、数据目录、Kafka Topic/消费组、ES 索引/alias、身份依赖。仅读取运行状态和非敏感配置；未读取或记录凭据。

## 主机资源

| 项目 | 观测值 |
| --- | --- |
| OS / 架构 | CentOS Linux 8.2.2004，x86_64 |
| CPU | 6 个逻辑 CPU；Intel Xeon E5-2680 v4 @ 2.40GHz |
| 内存 | 总量约 23 GiB；采集时可用约 19 GiB |
| Swap | 5 GiB；采集时已用约 1.7 GiB |
| 根盘 | `/dev/mapper/cl-root`，XFS，44 GiB；已用 15 GiB，可用 29 GiB（34% 已用） |
| 其他分区 | `/boot` 976 MiB ext4；EFI 599 MiB vfat |

单盘当前剩余空间满足“至少 30% 空闲”的初始运维目标，但 Kafka、PG、ES 和产品包共用根盘；仍需在 A03 容量预算中测算增长与磁盘保护水位。

## 服务与运行状态

| 组件 | 配置/版本 | 运行观测 | 状态判断 |
| --- | --- | --- | --- |
| TUBA API | Go 1.27.1；`/opt/tuba/bin/tuba-api` | 进程运行，监听 `127.0.0.1:8788`；readiness 已返回 200 | 正常；当前进程以 root 运行 |
| Kafka | 4.3.1；KRaft，`node.id=1`，`broker,controller`；API 配置指向 `127.0.0.1:9192` | Broker 进程运行；SASL_PLAINTEXT + SCRAM-SHA-256；9192、9193 监听通配地址 | 单节点 broker/controller；无副本冗余，网络加密及监听范围待 O03 收口 |
| Elasticsearch | 8.19.0；cluster `tuba-single-node`；`path.data=/var/lib/elasticsearch` | 1 节点、green、11 active shards、0 unassigned；HTTP 9200 与 transport 9300 监听通配地址 | 集群健康；单节点无副本，9200/9300 网络边界待 O03 收口 |
| PostgreSQL | 14.23；数据目录 `/opt/adms/postgresql/data` | 进程运行；5432 监听 `0.0.0.0` 和 IPv6 通配地址 | 连接正常；数据库端口网络边界待 O03 收口 |
| Web 反向代理 | `/opt/adms/webserver`，具体版本未从可见运行信息确认 | `webserver` master/worker 运行；监听 8443、8081、8099、9876 | 8443 为 TUBA 外部入口；8081 已确认为 ADMS 路由，不属于 TUBA |
| Keycloak | API 配置的 issuer 是 `http://10.6.68.247:8180/realms/tuba`；247 运行 Keycloak 26.7.4 | 248 上无 Keycloak 进程/监听；依赖 247 的开发 realm | 外部身份依赖；248 网络故障时无法新登录 |
| TUBA 数据面进程 | `tuba-ingest`、raw/standard/quarantine indexer、normalizer、analysis-sink、control-worker | 248 进程表中未发现这些运行实例；当前只观察到 `tuba-api` | 代码/二进制存在不等于服务正在运行；本轮现场不是完整数据面闭环 |

### 相关监听端口

| 地址/端口 | 进程 | 归属与说明 |
| --- | --- | --- |
| `127.0.0.1:8788` | `tuba-api` | TUBA API，loopback 私有监听 |
| `*:8443` | `webserver` | 外部访问入口，代理到 TUBA API |
| `0.0.0.0:9192`、`*:9193` | Kafka Java 进程 | SASL_PLAINTEXT broker 与 controller listener |
| `0.0.0.0:5432` | PostgreSQL | PG 接口当前绑定所有 IPv4 地址 |
| `*:9200`、`*:9300` | Elasticsearch Java 进程 | ES HTTP 与 transport 当前绑定通配地址 |
| `*:8081`、`*:8099`、`*:9876` | `webserver` | 既有 ADMS 服务监听；8081 不属于 TUBA |

## 数据目录

| 路径 | 采集时大小 | 说明 |
| --- | --- | --- |
| `/opt/tuba` | 约 964 MiB | API/ingest/indexer/sink 二进制、迁移、ES 模板和包缓存；包含约 655 MiB ES RPM、历史二进制备份及日志 |
| `/opt/adms/kafka/data` | 约 24 MiB | Kafka 消息数据 |
| `/opt/adms/postgresql/data` | 约 400 MiB | PostgreSQL 数据目录 |
| `/var/lib/elasticsearch` | 约 808 KiB | Elasticsearch 配置的数据目录，当前小规模测试索引 |
| `/opt/keycloak/data` | 248 上不存在/未使用 | Keycloak 独立在 247 |

248 上的 ES 模板目录目前只有 authentication/anomaly 相关模板和 ILM 文件；八领域统一模板、retention 安装和验证仍需按 C07/O02 完成。

## Kafka Topic 与消费组

Kafka Topic API 盘点共 26 个 Topic，其中 15 个为 TUBA 前缀，包含：

- 运行链路：`tuba.raw.events.v1`、`tuba.events.authentication.v1`、`tuba.events.invalid.v1`、`tuba.analysis.results.v1`。
- 验收隔离链路：`tuba.collector.validation.raw.v1`、`tuba.collector.validation.quarantine.v1`、`tuba.collector.validation.dlq.v1`，以及 authentication、directory、dns、iam、network、session、tls、web 八个领域的 validation Topic。
- 当前未发现正式命名的 `tuba.quarantine.v1`、`tuba.attributed.events.v1`、`tuba.indexing.dlq.v1`，也未发现 `tuba.events.{domain}.v1` 中除 authentication 外的正式领域 Topic。

| Topic | 分区数 | 副本数 | 观测 |
| --- | ---: | ---: | --- |
| `tuba.raw.events.v1` | 1 | 1 | 与 Topic 合同一致 |
| `tuba.events.authentication.v1` | 3 | 1 | 与 `contracts/events/topics.v1.json` 默认单分区不一致 |
| `tuba.analysis.results.v1` | 3 | 1 | 与 `contracts/events/topics.v1.json` 默认单分区不一致 |
| `tuba.events.invalid.v1` | 1 | 1 | API 环境中的 `KAFKA_DLQ_TOPIC` 指向此处 |
| `tuba.collector.validation.events.network.v1` | 1 | 1 | 隔离验收 Topic |

Topic 描述显示 `min.insync.replicas=1`。当前实际 DLQ 名称 `tuba.events.invalid.v1` 与合同 `tuba.indexing.dlq.v1` 不一致；后一 Topic 不存在。需在 C05/A04 统一运行配置、合同和隔离/生产 Topic，不能直接把历史 Topic 当成目标配置。

发现 6 个消费组，其中 4 个以 `tuba-` 开头；全部 TUBA validation 消费组报告 lag=0、没有活跃成员。Normalizer/Raw Indexer/Standard Indexer 的 group offsets 分别追到 16/16。它们证明既有隔离演练已追平，不代表当前消费者在运行。

## Elasticsearch 索引

ES API 使用服务端凭据只读检查，未输出密钥。共发现 11 个 TUBA 物理索引、41 条文档和 11 条 alias 映射（10 个不同 alias 名称）：

- `tenant_a` 有 16 条 Raw 证据；network alias 指向两个日期分区，分别有 4 条（`2024.07.14`）和 12 条（`2026.09.26`）。
- `n06_20260926_001` 隔离验收包含八领域 9 条标准事件，其中 network 2 条。
- 当前没有 `ueba-anomalies-tenant_a` 索引/alias，因此新租户异常概览必须把“索引尚未创建”视为 0 条，而不是存储故障；API 已按此修复。
- 运行目录的 ES 模板文件仅覆盖 authentication/anomaly，和八领域目标有差距。

## 需要进入后续任务的差异

1. A03 还需确认真实来源范围、峰值/日量、平均记录大小、Raw 保留期和异机备份目的地；本报告不推测这些业务参数。
2. C05/A04 对齐 `tuba.events.invalid.v1` 与目标 DLQ、Topic 分区数及 generation/隔离 namespace；确认没有旧消费者与新生产链路双写。
3. O01/O03/O04 补齐非 root 进程管理、Kafka/PG/ES 监听边界、传输保护、优雅停机及重连；不在本次只读盘点中改变配置。
4. O02/C07 扩展并重复执行安全的 Topic/ES 模板初始化，再验证八领域正式 Topic/alias；避免清理现有隔离验收数据。
5. A02 是运行快照：进程/lag/磁盘数据会变化；运行状态必须在部署或验收时重查，不能仅凭本报告视为持续健康。

## 2026-09-27 15:15 运行复查

从 21 和 248 只读复查运行状态，未更改服务或配置：

- 248 仍只有 `/opt/tuba/bin/tuba-api` 在运行；ingest、raw/standard indexer、normalizer、source-adapter 和 control-worker 均未运行。根盘仍为 44 GiB 总量、15 GiB 已用、29 GiB 可用。
- 248 Kafka 仍监听 9192/9193；`advertised.listeners=SASL_PLAINTEXT://localhost:9192`，配置行中没有 `authorizer.class.name`。API 仍只监听 `127.0.0.1:8788`。
- 21 上 Zeek 正常运行。Filebeat 8.19.0 的既有 `filebeat` systemd 服务 active，读取 `/etc/filebeat/filebeat.yml`、使用 `/var/lib/filebeat`；这是既有其他日志链路，不属于 TUBA。TUBA 隔离目录 `/opt/tuba-collector-validation` 存在，但无 TUBA Filebeat 进程。
- 从 21 发起 TCP 探测，248:9192 可建立连接，248:8788 不可连接（符合 API loopback 监听）；这只确认 TCP bootstrap 可达，不代表 Kafka SASL/元数据/Topic 权限可用。Kafka broker 返回 localhost advertised 地址仍使远端 Kafka 客户端不可用。
- 71:9092 经 Kafka API versions 查询确认为 Kafka 3.7.2，advertised address 为 `10.6.68.71:9092`，21 和 248 均可建立 TCP 连接。Topic 列表同时包含 ADMS/Filebeat/Winlogbeat topic 与既有 `tuba.n06_20260926_001.*` 隔离验证 topic；`kafka-acls --list` 返回 `SecurityDisabledException: No Authorizer is configured`。这是可访问的共享明文 broker，但没有 topic ACL 隔离，不能将真实 Zeek 日志发送到此 broker 并声称满足租户隔离。

因此下一步应为真实 Zeek 数据确定具备访问控制且不会影响现有 broker 使用方的远程路径，再准备隔离 Topic/凭据并启动 TUBA 专用数据面。71 可用于合成事件验证，不可作为真实日志安全隔离的证据。不要将 21 现有 Filebeat 或 248 API 的 loopback 端口当作闭环证据。

## 2026-09-27 Zeek Collector 合成闭环续验

本节记录 15:15 快照后的隔离验证进展；验证对象仍是合成 Zeek conn 事件，不代表真实 Zeek 流量已可安全送入 TUBA。

- 在 248 数据库执行迁移前，创建 `/opt/tuba/backups/pre-collector-migrations-20260927.dump` 备份，SHA-256 为 `d7be19f0ee066d40a3ee9e590abec2745cb69b6e0024bc8c0fe6e2113a411673`。对迁移 00007–00011 做事务回滚演练后按初始化脚本策略前滚，00001–00006 登记为基线；补齐 `source_instances`、`source_contexts` 等表。
- 创建独立组织/release/source contexts 和 `tuba.collector.zeek_validation_20260927_001.*` 验证 Topic。首次 context 的 dataset 缺少 `zeek.` 前缀，Normalizer 按 `DIP_ZEEK_DATASET_MISMATCH` 拒收；旧 context 已禁用并保留记录，新建 `zeek.conn`、`zeek.dns`、`zeek.http`、`zeek.ssl` contexts（ID 分别为 `ctx_500...005` 至 `ctx_800...008`）。不复用旧 context 修改其语义。
- 71 上本轮 Topic 均位于独立验证前缀，单分区、单副本、24 小时保留、2 MiB 消息上限。71 未启用 Kafka ACL，只接收合成验证数据；没有变更 71 或 248 的共享 Kafka 配置。
- 248 的手动启动器位于 `/opt/tuba/collector-validation-v2/zeek_validation_pipeline.py`，二进制位于同目录 `bin/`；使用独立验证配置/日志/运行状态目录，不覆盖 `/opt/tuba/bin`。启动器从运行中的 API 进程继承 DB/ES 运行环境，不把服务凭据写入新配置；`stop` 在宽限期后仅对记录的验证进程组升级为 SIGKILL。
- 第二轮 Filebeat 8.19.0 合成 conn 事件通过 21 隔离 Filebeat → 71 来源 Topic → 248 source-adapter/ingest/Raw/DIP/UIM → Kafka network Topic → Elasticsearch。adapter 收到 receipt 并提交 offset；ingest 接收 1 条、Raw indexer 写入 1 条，Raw alias 共 2 条（包括第一轮失败证据），network alias 有 1 条 qualified 标准事件。事件保留 `event.original`，Zeek 来源/目的 IP 和 300 bytes 与合成输入一致。
- 首轮 dataset 错误记录进入隔离 quarantine Topic；部署并启动 `tuba-quarantine-indexer` 后，ES alias `logs-ueba.quarantine-zeek_validation_20260927_001` 的 `_count` 为 1。验证进程随后通过启动器停止，状态为 `stopped`。
- 21 的临时 TUBA Filebeat（独立 `config-v2`、`data-v2`）PID 已停止；既有 systemd `filebeat` 服务复查仍为 `active`，未触碰其配置、registry 或进程。

结论：合成事件的 Collector → Raw/DIP/UIM → domain ES 与 quarantine → quarantine ES 两条验证路径都已走通。真实 Zeek 数据上线仍被 71 共享明文 Kafka 无 ACL 隔离、248 Kafka 远端 advertised listener 为 localhost、来源凭据/Topic ACL 管理未完成所阻塞；A04、COL-02/03/04、N03 与真实数据 V09 不得据本轮合成验收勾选完成。

## 2026-09-27 19:47 真实 Zeek 日志闭环快照

本节更新上述 15:15 快照。部署对象为 21 上 Zeek 的 conn/dns/http/ssl 四种 JSON 日志，输出目标是 248 新建的隔离 Kafka 和 `zeek_validation_20260927_001` ES namespace；不向 71 共享 Kafka发送真实事件。

- 248 新建手动控制的单节点 KRaft broker `10.6.68.248:29292`，启用 SCRAM-SHA-512 与 ACL；独立目录为 `/opt/tuba/collector-live/kafka`。没有改动原有 9192/9193 broker、配置或数据目录。四个 `tuba.source.<context>.v1` Topic、独立 Raw `raw.live2.v1`、事件/隔离 Topic 均为本轮专用。
- 来源 context：conn `ctx_9173765dafede7b01176206fc49f70d9`；dns `ctx_df595c138c0ecac68cf9e8af95b7b881`；http `ctx_5abc8a06f8a0088878248d4504d22a62`；ssl `ctx_5fadf1a689e7900ff74af27b5674c1c5`。每路 Filebeat 使用专属 SCRAM 用户与精确写入 ACL；source-adapter 经 ingest 收到持久 receipt 后提交对应 Kafka offset。
- 21 的专用管理器 `/opt/tuba/collector-live/filebeat-r2/manage_zeek_filebeat.py` 启动四个独立 Filebeat 进程，各自拥有配置、registry、磁盘队列和日志；还管理一个每 30 秒检查归档的 `archive-sync` 子进程。活动文件直接读取 `/opt/zeek/spool/zeek/{conn,dns,http,ssl}.log`；最近 90 分钟 `.log.gz` 先完整 gzip 校验并解压到 `/opt/tuba/collector-live/filebeat-r2/archive/<dataset>/`，成功后原子改名供 Filebeat 读取。发现 8.19 filestream 不支持 gzip 直读；首轮曾把压缩字节当日志发送，修正后归档 sample 为有效 Zeek JSON，Raw 中的 `event.original` 与 `message` 一致。初轮错误证据未删除。
- 248 手动 TUBA 数据面位于 `/opt/tuba/collector-live/pipeline`，六个组件由 `/opt/tuba/collector-live/manage_zeek_live_pipeline.py` 控制。Raw 以 `raw.live2.v1` 独立消费组继续入库；Normalizer Kafka writer 小批等待设为 10 ms，消费 lag 随后追平。原有 API/服务环境未被替换。**（2026-09-30 更新：本行是 2026-09-27 的只读快照。该 Python 监督器已不再运行数据面——六个 Zeek 组件连同四个 tenant_a 组件与 api 共 11 个服务，现由产品 Launcher 按 `/etc/tuba/tuba-services.json` 统一监督；两个监督器脚本保留在磁盘上仅作回滚退路。二进制路径与 `/opt/tuba/collector-live/pipeline` 布局未变，故此节其余测量值仍有效。见 [实施 TODO](IMPLEMENTATION-TODO.md) O01 与 [运维手册](RUNBOOK.md)。）**
- 21 上既有 systemd `filebeat` 持续为 `active`；检查确认它的 `/etc/filebeat/filebeat.yml`、`/var/lib/filebeat` 和服务没有变更。新 TUBA Filebeat 使用独立配置和 data 根目录。
- 验收快照的四个 source Topic 消费组均 lag=0；Raw Topic end offset 为 51,270。Raw alias `_count` 为 51,665（含之前隔离验收数据）；network/dns/web/tls alias 分别 33,012 / 10,048 / 139 / 371 条；quarantine alias 8,140 条。r2 source-adapter、Raw indexer、Normalizer 消费组均追到输入末尾；标准 indexer 的 network 流当时有 2 条在途 offset，其余领域 lag=0。r2 Raw-indexer DLQ 无新增；隔离 DLQ Topic 中保留了早期旧轮次 423 条。
- quarantine 原因聚合：`DIP_ZEEK_ORIGINAL_INVALID` 4,295 条来自首次误读 gzip 的故障轮，`UIM_TLS_SERVER_NAME_REQUIRED` 3,574、`UIM_DNS_QUERY_REQUIRED` 44、`UIM_HTTP_HOST_REQUIRED` 45 来自当前 UIM 必需字段约束；早期 `PAYLOAD_HASH_MISMATCH` 181 与 `DIP_ZEEK_DATASET_MISMATCH` 1 保留为历史失败证据。解压修正后 `DIP_ZEEK_ORIGINAL_INVALID` 未继续增长。无 SNI/query/host 的记录仍有 Raw 证据，但没有进入标准域索引。
- 运行限制：broker 只有一个节点，不具备故障冗余；开发链路按当前要求使用 HTTP。当前只采 conn/dns/http/ssl 活动文件及最近 90 分钟已轮转文件；归档解压 spool 未配置自动清理，需在 A03/COL-07 定义容量、保留和长时间停机后的补采边界。UIM 可选字段规则是否改为 partial 应单独评审。采集过滤目前只按 dataset 和归档时间范围选择，不执行语义过滤规则。

结论：Zeek 四数据集的真实文件→Filebeat→隔离 Kafka→receipt/offset→Raw→DIP/UIM→ES 闭环已运行，首轮错误也保留并可审计。此结论不关闭通用 collector 阶段、Winlogbeat/Syslog/远程管理、多节点 HA、容量/RPO 或完整 V09 业务闭环任务。

## 2026-09-27 21:35 隔离 Kafka ACL 与消费组复核

针对 C05 在 248:29292 做只读核对，并在隔离 broker 上运行一次临时默认拒绝探针；未改动原 9192/9193 broker。

- `StandardAuthorizer` 正在运行，broker 配置 `allow.everyone.if.no.acl.found=false`；单节点默认保留 24h、消息上限 2 MiB。
- conn/dns/http/ssl 四个实际 source Topic 均为 1 partition。topic 级无动态配置覆盖，使用 broker 默认值。各来源 `tuba-zeek-{conn,dns,http,ssl}` 仅在各自 literal Topic 上有 `WRITE`、`DESCRIBE` ACL。
- consumer group ID 与合同一致：`tuba-source-adapter-<topic SHA-256 前 16 个 hex>-zeeklive20260927r2`。但这次复核时四个 r2 group 都没有活跃 member，lag 分别为 conn 1,732、dns 508、http 18、ssl 200；这是当前积压快照，19:47 的 lag=0 只代表此前验收时点，不能代表现在已追平。早期 placeholder context group 也留有积压：ssl 3,928、dns 9,557、http 206。
- `scripts/verify_kafka_acl_default_deny.sh` 在隔离 broker 上创建临时 SCRAM 身份和 60 秒保留的 disposable Topic；身份成功认证后写入被 Kafka 的 Topic ACL 拒绝。脚本退出后，复查确认临时 Topic 和临时身份均已清理。
- 这证明 default-deny 行为和 source producer 的 literal ACL，但尚未证明正式全 Topic 目录与机器合同逐项一致；手工 broker 管理脚本仍使用 worker 的 topic/group 前缀 ACL，和合同中的精确 adapter group ACL 尚需收口。容量/RPO 也仍未定版。

本节快照后，source-adapter 已恢复，ACL 前缀也已收口；详细复验见下节。C05 仍未关闭，剩余全 Topic 保留期与合同/容量定版。

## 2026-09-27 22:00 C05 恢复与精确 ACL 收口

随后检查发现其余五个数据面组件仍运行，只有 source-adapter PID 已退出。最后日志记录为 Kafka offset commit 时连接关闭（`use of closed network connection`）。使用现存 `tuba-ingest` 进程的环境变量（凭据仅在内存传递），单独重启 source-adapter，未重启其他消费者。

- 四个 r2 source group 重新加入，各自追到当时 Topic end offset，lag=0；四个 source Topic 仍有 active member。当前索引链路 15 个组件 group 均有 active member；后续汇总时点报告的 active lag 为 2，属持续采集过程中的短暂在途消息。旧 Raw Indexer 与 Normalizer group 无 member，保留的历史 lag 分别为 1,682 和 2,060；未重置或删除其 offsets。
- 修改后的 `scripts/manage_zeek_ingress_broker.py reconcile-acls` 先为实际四个 source Topic 和当前 15 个 consumer group 添加 literal ACL，再移除 `User:tuba-zeek-worker` 的 `tuba.source.` topic 前缀和 `tuba-` group 前缀 ACL。复查确认两条前缀 ACL 不存在、15/15 个当前 group 精确 ACL 存在；Filebeat 用户继续只对各自来源 Topic 有 `WRITE`/`DESCRIBE`。
- Kafka 默认拒绝探针已通过，并确认探针 topic/user 清理完成。ACL 变更没有重启 broker或改变消息数据。
- Topic 保留配置仍有差异：source Topic 使用 broker 默认 24h/2 MiB；管理脚本创建的 Raw、domain、quarantine、DLQ 隔离 Topic 也统一使用 24h/2 MiB，而目录目标分别是 7d 或 14d。该隔离配置暂不扩到正式运行配置，需先由 A03 确定容量预算和保留期。

C05 仍不勾选：source ACL、default-deny 和消费组运行证据已补齐；还需明确隔离/正式 Topic 的 retention profile，并等待 A03 容量/RPO 决策后完成正式全目录配置对账。

## 2026-09-27 ES 旧 authentication 目标只读复查

为 C07 切换验收补查 248 Elasticsearch：

- 集群版本为 Elasticsearch 8.19.0。`GET /_data_stream` 返回 HTTP 200、`data_streams: []`，确认当前没有任何 Data Stream；先前的 405 来自 `GET /_cat/data_streams`（该集群对此 CAT API 只接受 POST），不是 `_data_stream` API 不可用。
- `GET /_cat/indices` 与 `GET /_cat/aliases` 均返回 HTTP 200。authentication 逻辑 alias `logs-ueba.authentication-n06_20260926_001` 指向物理索引 `tuba-v1-uim-authentication-n06_20260926_001-g1-2026.09.26`；该物理索引存在且有 1 条文档，mapping 含 `@timestamp: date`。没有同名旧 Data Stream，也无需对该索引执行迁移或删除。
- 所有核查均为只读，没有修改索引、alias、模板或数据。C07 的旧 Data Stream 冲突条件已通过“确认不存在”解决；模板生成/漂移校验由仓库脚本负责，正式 retention 配置仍受 A03/C05 保留预算约束。

## 2026-09-27 22:27 容量运行快照

只读复查 248 文件系统、TUBA 隔离 Kafka 目录与 ES 索引：根盘 44 GiB 总量、17 GiB 已用、28 GiB 可用；`/opt/tuba/collector-live/kafka` 约 715 MiB，`/var/lib/elasticsearch` 约 405 MiB。Zeek validation Raw 索引 121,756 条/226.1 MiB，network 82,494 条/125.5 MiB，dns 24,365 条/33.6 MiB，web 344 条/1.3 MiB，tls 924 条/1.9 MiB，quarantine 13,708 条/11.4 MiB。链路持续写入，此为累计运行快照而非完整日量；没有更改服务、数据或 retention。容量样本与限制整理在 [CAPACITY-OBSERVATION-20260927.md](CAPACITY-OBSERVATION-20260927.md)。

## 2026-09-27 22:34 C05 Topic/ACL 实际目录复查

只读查询 248:29292 隔离 broker 的 `kafka-topics --list/--describe` 和 `kafka-acls --list`：当前有 21 个 TUBA Topic（不含 Kafka 内部 `__consumer_offsets`），各为 1 partition/1 replica。物理目录包含 namespace 下 Raw、8 个 domain、Quarantine、DLQ、source-adapter DLQ、旧 `raw.v1` 及当前 `raw.live2.v1`，另有 8 个 source-context Topic（4 个当前上下文和 4 个早期占位上下文）。当前物理 Topic 最大消息配置为 2 MiB；内部 Raw/domain/quarantine/DLQ Topic 为 24h，source Topic 显式或继承 broker 默认 24h。合同保留目标分别是 source 24h（仍 provisional）、Raw/domain 7d、Quarantine/DLQ 14d，尚未对齐。

- 四个当前 Zeek Filebeat 身份只对各自 source Topic 有 literal `WRITE`/`DESCRIBE`；旧占位 source credentials 也仅对各自 Topic 有相同规则。
- `tuba-zeek-worker` 对当前 15 个 consumer groups 有 literal `READ` ACL；此前 `tuba-` group 前缀 ACL 已移除。但数据面所有组件仍共用该身份，它对 namespace 内 `tuba.collector.<namespace>.` 具有 `READ`/`WRITE`/`DESCRIBE` 前缀 ACL，并对 source Topics 具有 `READ`/`DESCRIBE`。这是 validation namespace 的共用运行身份，不等同于合同所列服务角色的逐服务最小权限。
- 合同现增加 `physical_name` 映射：逻辑 topic 名在 validation namespace 中如何解析为物理名已机器校验，详情见 `contracts/events/topics.v1.json` 与 `TARGET-ARCHITECTURE.md`。该映射解决命名歧义，但还未把当前配置差异变成通过验收。

C05 仍未关闭：A03 的容量与恢复决策未定，Topic retention 未按容量定版；隔离 broker 还缺逐服务 ACL 身份及逐项配置对账。未改动 broker 配置、Topic 数据或 ACL。

## 2026-09-27 23:16 C05 逐服务身份和 ACL 验收

在保留 Kafka 数据、offset 和现有 Filebeat 进程的情况下完成 validation data-plane 身份切换。先写入 6 个服务 SCRAM 身份及精确 ACL，确认各组件新身份已启动后，才移除旧共享 `tuba-zeek-worker` 的 namespace Topic 前缀授权；旧消费者组 offset 没有重置。

- 服务 ACL 数量按 Topic/group 核验：ingest 1/0，source-adapter 5/4，raw-indexer 2/1，normalizer 10/1，quarantine-indexer 2/1，standard-indexer 9/8。每项均为 literal 资源规则，覆盖相应输入、输出与消费组；6 个 TUBA 组件分别使用这些身份。配置保存在 248 root-only `secrets.json`，没有回显凭据。
- 四个活动 Filebeat 身份现在各自只有 1 个 literal source Topic `WRITE`/`DESCRIBE`；撤销了它们对早期 placeholder context 的旧写权限。`tuba-zeek-worker` 不再有任何 PREFIXED ACL，只剩 5 个历史 Topic 与 6 个旧 group 的 literal READ/必要 DESCRIBE，用于访问遗留 backlog；没有生产权限。
- 重启后 6/6 数据面进程运行，当前 15/15 消费组均有 active member；当时报告 lag 合计 4，来自持续接入的在途记录。进程日志中 SASL/Topic/Group 授权错误计数为 0。4 个当前 source groups 与标准/Raw/Normalizer/Quarantine 消费组保持正常。旧 placeholder source group 的 lag 仍存在（当时分别 303、14,176、5,575），旧 Raw/Normalizer group 也未重置或清理。
- 默认拒绝探针此前已验证无 Topic ACL 的认证身份不能写入 disposable topic；临时 topic/user 均清理。ES、PG、Kafka broker 和 Topic retention 未重启或改动。

validation Topic 参数与 [topics.v1.json](../contracts/events/topics.v1.json) 的 `zeek_validation_single_node` profile 对齐：1 partition、1 replica、2 MiB、24h（source Topic 使用 broker 默认值）、manual commit、at-least-once、Kafka transactions 关闭。合同产品目标仍为 Raw/domain 7d、Quarantine/DLQ 14d；该 24h profile 仅限 bounded validation，生产 retention 需 A03 容量/RPO 预算，不作生产承诺。当前 21 个 TUBA Topic 与索引配置的只读盘点见本节前文。
