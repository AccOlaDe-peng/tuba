# Zeek 输入容量观察（2026-09-27）

采集时间：2026-09-27 08:42–08:44（Asia/Shanghai）  
来源：`10.6.69.21`，Zeek 日志目录 `/opt/zeek/logs` 和活动 spool `/opt/zeek/spool/zeek`。仅读取文件元数据和逐行长度/数量；没有输出日志内容。

## 实测样本

Zeek 以 3600 秒轮转日志并 gzip 归档。`/opt/zeek/logs` 当时约 411 MiB，归档日期覆盖 2026-09-10 至 2026-09-27；活动 spool 约 6 MiB。来源主机 Ubuntu 20.04.3，8 个逻辑 CPU、35 GiB 内存，根盘 196 GiB、可用 140 GiB。

| 样本日 | 归档压缩量 | 归档解压后字节 | 数据行数 | 全日志平均记录 | 24h 平均 EPS | 峰值小时记录 / EPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-09-16（可见归档中最高） | 44.96 MiB | 408.60 MiB | 1,142,506 | 358 B | 13.22 | 59,187 / 16.44 |
| 2026-09-26（近期完整日） | 19.17 MiB | 194.54 MiB | 709,356 | 274 B | 8.21 | 30,208 / 8.39 |

2026-09-16 首批目标领域 conn/dns/http/ssl 合计 706,064 行、279.14 MiB 解压后数据、31.44 MiB gzip；平均约 8.17 EPS、平均行长约 395 B。其余记录包含 telemetry、files、arp、weird、DHCP 和 Zeek 状态/元数据日志，是否接入必须按产品用途和过滤策略决定，不能把所有日志默认送入 TUBA。

活动 spool 在 08:43 时 conn.log 约 2.22 MiB、dns.log 约 0.70 MiB；它们是当前轮转周期中的增长快照，不作为完整日量外推依据。

## 初步容量边界

目标架构目前给出的初始保留目标为 Raw 30 天、标准事件 90 天。按 2026-09-16 观测值做未压缩源数据下界估算：

- 若 Raw 接收全部 Zeek 日志，30 天约 `408.60 MiB × 30 = 12.0 GiB`。
- 若标准链路只接 conn/dns/http/ssl，90 天约 `279.14 MiB × 90 = 24.5 GiB`。
- 两者合计约 36.5 GiB 未压缩数据，尚未计入 envelope、ES/Kafka/PostgreSQL 元数据、segment/translog、索引结构、更新/恢复空间及其他来源。248 根盘当前可用约 29 GiB。

这是数据量级对比，不是 ES/Kafka 磁盘承诺：gzip 归档比率不能直接换算为 ES store size，ES codec、字段映射、Kafka 压缩和实际 Raw envelope 都会改变落盘量。现有样本还未测 Windows Security，也不是全年峰值；须用实际 Collector shadow 计数、标准化输出和 ES/Kafka store bytes 校准后再定容量。

## A03 尚待确认

1. 首期采集来源/Zeek dataset：只采 conn/dns/http/ssl，还是还需 files/arp/weird/telemetry 等；Windows Security 的事件范围也需纳入预算。
2. Raw 与标准事件实际保留时长，以及是否保留全部 Raw；变更保留目标会线性改变基线估算。
3. 248 是否扩容数据盘或调整保留/采集范围，以覆盖当前未压缩下界并保留运维空闲空间。
4. 异机备份目的地与目标 RPO/RTO；本次没有发现可据以承诺的备份目标或恢复水位。

因此本记录只完成流量实测和量级估算，A03 不关闭。避免在采集/保留边界和备份目的地未定时把单日样本误当成容量承诺。

## Windows Security 24 小时体量观察（2026-09-27 22:00）

通过 WinRM 对 139 域控和 169 组成员远端汇总最近 24 小时的重点 Windows Security 事件。仅返回事件 ID 数量、远端 XML 字节合计和日志元数据；未把事件正文传回本机。测量 ID 集合为 4624、4625、4648、4672、4688、4719、4720、4722、4723、4724、4728、4729、4732、4733、4768、4769、4771、4776、5140，表示候选采集范围，不代表最终发布过滤规则。

| 主机角色 | Security 当前记录数 | 最大日志容量 / 模式 | 候选 ID 最近 24h 数 | 事件 XML 字节合计 |
| --- | ---: | --- | ---: | ---: |
| 139 域控制器（WIN-139） | 217,170 | 0.12 GiB / Circular | 6,642 | 10,512,563（约 10.0 MiB） |
| 169 组成员（WIN-169） | 33,190 | 0.02 GiB / Circular | 1,518 | 2,753,978（约 2.6 MiB） |

139 的主要记录为 4624 登录成功 4,829 条（约 8.1 MiB XML）和 4719 审计策略更改 1,728 条（约 1.8 MiB）；4719 比例偏高，是否纳入检测用途应复核策略/采集目标。169 主要为 4624 登录成功 1,507 条。合计重点事件约 8,160 条、12.6 MiB XML/日。Windows 安全日志为循环覆盖，当前最大日志空间合计约 0.14 GiB；记录数不能直接换算可保留天数。

XML 字节只用于粗略下界，Winlogbeat ECS JSON、附加字段、压缩与 TUBA envelope 后的实发消息大小尚未测量。正式采集范围应由事件语义和检测试例确定，再用 Winlogbeat shadow 计数/编码尺寸校准。按这次观察，Windows 候选事件量明显低于 Zeek 原始输入；主要单节点容量风险仍来自 Zeek Raw＋ES 标准保留及 ES/Kafka 磁盘增长。

## A03 决策仍开放

- Windows 两台主机的候选事件量已有量测，但事件 ID/日志来源的最终采集范围未定；139 上 4719 应先确认用途。
- Raw、标准事件、Quarantine、Kafka ingress 的正式保留期仍未由容量/RPO 决定；Zeek 粗略未压缩下界 36.5 GiB 已超过 248 在 2026-09-28 15:42 约 22.65 GiB 可用空间，尚未计 ES/Kafka 放大与运维空闲空间。
- 248 数据盘扩容方案、异机备份目的地及 RPO/RTO 均未确认。未把测试环境本地 migration dump 当成异机备份证据。

## 248 实际存储快照（2026-09-27 22:27 CST）

在 248 只读查看文件系统及 ES 索引：根盘 44 GiB 总量、17 GiB 已用、28 GiB 可用；隔离 Kafka 数据目录约 715 MiB，ES 数据目录约 405 MiB；`tuba-v1-*` ES 索引合计约 0.4 GiB。Zeek validation Raw 索引有 121,756 条、226.1 MiB；network 82,494 条、125.5 MiB；dns 24,365 条、33.6 MiB；web 344 条、1.3 MiB；tls 924 条、1.9 MiB；quarantine 13,708 条、11.4 MiB。该快照是实时运行中的累计量，未对应完整 24 小时窗口，不能单独用作日均外推；它表明 Elasticsearch 的实际存储尺寸与原始 Zeek 日志解压字节并非同一量纲。未修改服务或保留配置。

因此 A03 当前已有源端流量样本、Windows 候选事件观察和目标机实际目录/索引快照；还需按完整运行窗口测得原始/标准事件日增量及 Kafka 增长，才能定保留与扩盘预算。异机备份目的地、恢复演练目标 RPO/RTO 尚无依据，继续保持未决。

## 248 短窗口增长复测（2026-09-27 22:27–23:16 CST）

同一 248 根盘与数据目录的两次只读快照相隔约 49 分钟：Kafka 目录由约 715 MiB 增至 848 MiB（+133 MiB），ES 数据目录由约 405 MiB 增至 463 MiB（+58 MiB），根盘可用量从约 28 GiB 变为约 27 GiB。Raw 索引由 121,756 条/226.1 MiB 增至 144,026 条/266.2 MiB；network 由 82,494 条/125.5 MiB 增至 98,225 条/139.8 MiB；dns 由 24,365 条/33.6 MiB 增至 28,984 条/36.3 MiB；quarantine 由 13,708 条/11.4 MiB 增至 15,316 条/12.1 MiB。服务持续接收并经历 pipeline 身份滚动，49 分钟数据不能视为平稳日量外推，但已经证明 Kafka/ES 存储仍增长，需用完整日和连续多日基线复核容量。短期复测未改变 A03 结论：当前盘不支持把 30d/90d 保留作为已验证预算，异机备份目标和恢复指标仍需定案。

## 248 / Zeek 最新容量快照（2026-09-28 13:54 CST）

本次恢复主机连通后再次只读采样，未读取事件内容、未改服务或保留配置：21 根盘 196 GiB、可用 140 GiB；`/opt/zeek/logs` 约 437 MiB、`/opt/zeek/spool/zeek` 约 13 MiB。248 根盘仍为 44 GiB，总已用约 21 GiB、可用约 24 GiB；隔离 Kafka 目录约 2.8 GiB，ES 数据目录约 1.4 GiB。

与 2026-09-27 23:16 快照相比（间隔约 14 小时 38 分），248 Kafka 目录约增加 2.0 GiB、ES 目录约增加 0.9 GiB、根盘可用约减少 3 GiB；与此前报告中的 27 GiB 可用快照相比，当前也进一步下降。这个累计差值不能直接按日外推：期间可能有重放、历史积压消费或其他写入，且没有同步记录每个 Topic、alias 的起止水位。它足以确认 248 当前空间余量继续收窄，部署前需查明增长归属并设定告警/停止阈值。

主机进程名快照显示 248 上当前存在 TUBA API、ingest、normalizer、source-adapter、Raw/Quarantine/Standard indexer 等运行进程；本次只统计进程名称，没有读取进程参数或环境变量。该事实表明隔离链路仍在运行，不等同于容量或可靠性验收完成。A03 仍需连续按时段记录 Kafka/ES/Raw/domain 日增量、确认 collector spool 清理水位，并由业务方确定保留、扩盘和异机恢复指标。

## 248 根盘精确复测（2026-09-28 14:29 CST）

再次仅读取磁盘与目录元数据：根盘可用 `23,697,504 KiB`（约 22.6 GiB）；隔离 Kafka 目录 `3,149,624 KiB`（约 3.00 GiB）；ES 数据目录 `1,508,776 KiB`（约 1.44 GiB）。13:54 记录使用 `du -sh`/`df -h` 的一位小数/整数显示，不能据此精确计算 35 分钟增量；本次只作为更精确的当前占用基线。仍需对相同口径的连续样本、Topic/索引水位和 retention 变化做归因，现有数据不用于日量线性外推。

## 248 只读复测（2026-09-28 15:42 CST）

再次只读查询根盘、隔离 Kafka/ES 目录及 ES 聚合索引元数据，没有读取事件正文、修改服务或变更保留配置。根盘总量约 43.4 GiB、可用 `23,751,308 KiB`（约 22.65 GiB）；Kafka 目录 `3,137,932 KiB`（约 2.99 GiB）；ES 数据目录 `1,623,948 KiB`（约 1.55 GiB）。ES 为单节点 green，24 个 active shard、0 个 unassigned shard。与 14:29 的精确快照相比，ES 目录增加约 112.5 MiB，Kafka 目录减少约 11.4 MiB；这 73 分钟的差异可能包含 ES 写入、Kafka segment 回收/压缩和其它运行影响，不外推为稳定日量。

ES 元数据显示 Zeek validation 的 Raw 索引在 2026-09-27/28 分别有 351,356/275,854 条，store 约 589.2/480.5 MiB；network 标准索引分别有 253,799/168,661 条，store 约 216.3/145.5 MiB，DNS/Web/TLS 与隔离索引也持续增长。9 月 28 日是未结束的自然日，9 月 27 日索引也不一定覆盖完整采集日，因此这些索引统计只证明仍有数据写入，不能直接作为完整日量。

本次没有消除 A03 阻塞：根盘可用空间仍低于 Raw 30 天＋标准事件 90 天的未压缩下界约 36.5 GiB，且 ES/Kafka 放大、系统预留空间、Windows 数据及其它 TUBA 数据尚未计入。生产部署前仍须定最终事件范围与保留期、扩盘预算、备份目标及 RPO/RTO；在此之前不启用生产 retention profile。

## A03 当前执行边界（2026-09-28）

此前 128/160 GiB 是基于候选 30d Raw＋90d 标准保留的粗略扩盘估值；用户已明确当前不需要扩盘建议，因此该估值不作为推荐、批准或后续默认方案。当前不制定扩盘动作。

现有证据只能得出：248 根盘可用约 22.65 GiB，且 Zeek Raw 30d＋标准事件 90d 的未压缩下界约 36.5 GiB，尚未计 ES/Kafka 放大与系统安全余量，故该保留组合无法在现有盘上成立。把两个保留目标同时启用会带来容量风险；但目前也没有足够连续完整日的同口径增长数据，不能诚实地给出一个新的安全保留天数。

后续按现有磁盘推进：

1. 保持 248 当前 validation 范围，不扩展来源或事件类型，不启用未经 A03 核准的生产保留 profile；在明确停止/清理策略前不把持续增长当作可长期接受状态。
2. 从连续完整自然日采集同口径的 Kafka 各 Topic bytes、ES Raw/domain/Quarantine store bytes、行数、source spool 大小及剩余磁盘；标注回放、补数、索引 rollover 等扰动，不用短时目录差值外推。
3. 以实际净增长和可用空间推导最大保留窗口，并为 OS、重建 shard、Kafka segment 和故障恢复留下安全余量；再据此决定采集范围及 Raw/domain/Quarantine 各自保留期。达不到业务最小保留要求时，需减少接入范围或暂停非必要数据流量，由业务明确取舍。
4. 补齐 21 解压 spool 的有界清理与磁盘水位告警；容量保护应先告警并阻止新增/非必要数据，不能静默删除原始证据。异机备份目的地和 RPO/RTO 仍需单独确认。

A03 仍开放。未将容量估值转化为扩盘建议，也未在 248 修改 retention、删除数据或部署监控栈。

## A03 最终容量边界（2026-09-28 18:40 CST）

本节取代上文所有“待确认”建议，作为当前 50 GiB 单节点开发环境的容量决定。它不承诺磁盘故障恢复，也不授权扩大来源范围。

### 最新同口径实测

248 根文件系统总计 `46,588,542,976` B（43.39 GiB），已用 `23,745,736,704` B（22.11 GiB），可用 `22,842,806,272` B（21.27 GiB）。TUBA 隔离 Kafka 目录约 `4,032,688,128` B（3.76 GiB），ES 数据目录约 `1,957,481,280` B（1.82 GiB）；ES 单节点 green，24 个 active primary shard、0 unassigned。

Kafka broker 默认和已显式配置的 TUBA Topic retention 都是 24 小时。当前主要 Kafka 落盘约为 Raw 1.95 GiB、network 0.78 GiB、DNS 0.21 GiB、Web 0.11 GiB，加上 source Topic、Quarantine 和 consumer offsets 后合计约 3.76 GiB。该结果证明 Kafka 7/14 天保留不适合当前磁盘。

ES 的 2026-09-27 Zeek Raw、network、DNS、Web、TLS、Quarantine 合计约 0.86 GiB；2026-09-28 截至 18:40 合计约 0.96 GiB，按已过时间粗略折算约 1.24 GiB/日。为覆盖流量变化，容量预算使用 1.4 GiB/日，不使用较低均值。

21 根盘约 195.37 GiB、可用约 139.00 GiB。TUBA Filebeat r2 目录约 171 MiB，其中解压 archive stage 约 131 MiB、四路 Filebeat data 约 21 MiB。每个 dataset 的磁盘队列上限为 256 MB，四路合计硬上限约 1 GiB；这是字节上限，不承诺固定离线时长。archive stage 现规定最多保留 6 小时，源端 Zeek gzip 归档仍是补采依据。

### 批准范围与保留

| 层 | 当前批准范围 | 保留/上限 |
| --- | --- | --- |
| 来源 | 仅 21 的 Zeek `conn/dns/http/ssl` | 不增加 dataset；Windows、Syslog、JumpServer、Keycloak 启用前重新测量 |
| Filebeat queue | 四个独立磁盘队列 | 每路 256 MB，合计约 1 GiB |
| 解压 archive stage | 最近归档的临时解压副本 | 6 小时；过期 stage 自动清理，源 gzip 不由 TUBA 删除 |
| 所有 TUBA Kafka Topic | source、Raw、domain、Quarantine、DLQ、analysis | 24 小时；1 partition、1 replica |
| ES Raw | 当前 namespace/generation 的 UTC 日索引 | 最多 7 个自然日分区 |
| ES domain | network/dns/web/tls；其余当前为空 | 最多 7 个自然日分区 |
| ES Quarantine | 当前 namespace/generation | 最多 7 个自然日分区 |
| 后续 attribution/feature/anomaly/risk | 当前不启用 | 启用前做增量容量测量；启用后的初始上限仍为 7 日 |

按保守值估算，Kafka 24 小时约 3.8 GiB，ES 7 日约 9.8 GiB，合计约 13.6 GiB。扣除当前非 TUBA 使用量并保留根盘 20% 空闲后，TUBA 可用预算约 18.2 GiB，尚留约 4.6 GiB 给 segment 回收、translog、日志和测量误差。因此 7 日是当前批准上限，不是可以继续叠加新来源的余量。

ES 删除按 UTC 日物理索引执行：只在第 8 个分区出现后删除最旧已关闭分区，并继续执行 `no_active_job_lease`、备份策略和审计保护；不得按文档逐条删除。Kafka retention 到期可造成不可恢复的重放边界，消费者不得静默跳到 latest。

### 磁盘保护水位

- 根盘使用率达到 70%（约剩 13.0 GiB）告警并冻结新来源、补采和回放。
- 达到 75%（约剩 10.8 GiB）进入 critical，要求处理积压、核对 retention job 和异常增长。
- 达到 80%（约剩 8.7 GiB）停止新接入写入和非必要任务，让已确认数据排空；不得依靠静默删除 Raw、Quarantine 或 DLQ 恢复空间。
- 任何单日实际净增长超过 1.4 GiB，或 Kafka 24 小时占用超过 4.5 GiB，立即重新打开 A03；在复核前保持来源冻结。

### 恢复限制

Kafka 的硬回放窗口为 24 小时。消费 lag age 达到 6 小时告警、12 小时 critical、18 小时必须停止来源或进入受控恢复，避免越过 retention。Filebeat 的离线能力以每路 256 MB 队列为准，不能换算成保证小时数；队列耗尽后依靠 Zeek 原始 gzip 做显式补采并生成缺口/回放记录。

当前没有异机备份目标，Kafka 和 ES 都是单副本。因此：进程或短时依赖故障在磁盘完好且未超过上述窗口时目标 RPO 为 0；248 磁盘或整机丢失时没有可承诺的 RPO/RTO，可能丢失全部 TUBA 数据。该限制已经明确，允许关闭当前开发单节点的 A03，但不构成生产灾备验收；异机备份与恢复演练继续由 O05/V07 跟踪。

本次只读核查没有修改 248 retention 或删除数据。合同和生成资产已将 Kafka 统一为 24 小时、ES 当前数据层统一为 7 日；实际应用 retention 和保护水位属于 O05 部署步骤。

## O05 应用记录（2026-09-28）

248 已实际运行容量守护进程，使用产品自带管理 CLI start/stop/status，不注册 systemd。它每 60 秒检查 `/`，仅清理 `zeek_validation_20260927_001` namespace 下超过 7 日的 Raw、Quarantine 和八领域 UTC 日索引，并在 `127.0.0.1:19100` 暴露健康和 Prometheus 指标。ES persistent disk watermarks 已设置并读回为 low 70%、high 75%、flood stage 80%；达到 flood stage 时 Elasticsearch 自动设置只读但允许删除的保护块。

部署验收时根盘使用率约 50.8%，状态 `normal`，删除候选 0，ready 返回 200。首次启动的匹配范围未限制 namespace，依据 7 日规则删除了一个 2024-07-14 的旧 `tenant_a` network 样例索引（4 条文档）。该删除已写入 audit JSONL，但没有备份，不能恢复；发现后守护进程已停止、规则收窄为当前批准 namespace、重新启动并读回候选为 0。后续不会处理其它 namespace。

当前“告警”交付为本机状态变化日志和 Prometheus 指标；尚无 Prometheus/Grafana 或外部通知接收器，因此 O05 总任务仍保持开放。Kafka Topic 已处于 24h broker/topic retention，本轮未重启 Kafka、未修改业务进程和现存事件数据。

### 监控栈部署进度（2026-09-28）

248 已运行 Prometheus 3.5.0、node_exporter 1.9.1、Kafka exporter 1.9.0、Grafana 12.2.0；服务由 `/opt/tuba/monitoring/manage_tuba_monitoring.py start|stop|status [all|service]` 管理，不注册 systemd。Prometheus TSDB 上限 15 日/1 GiB；Prometheus 19090、node exporter 19101、Kafka exporter 19102、Grafana 13000 和 capacity guard 19100 均经 `ss` 核实仅监听 `127.0.0.1`。进程分别由 `tuba-prometheus`、`tuba-node-exporter`、`tuba-kafka-exporter`、`tuba-grafana` 等 nologin 用户运行。Prometheus 配置及 8 条告警规则经 promtool 检查，现场可达的 5/5 scrape targets 均为 UP。Grafana `/api/health` 返回 200；Prometheus datasource 和 TUBA Operations dashboard 已完成 API 读回。官方 SHA-256 sidecar 已匹配 Prometheus、node_exporter、Grafana 包；Kafka exporter v1.9.0 release 没有官方 digest，远端下载文件与工作站官方 release 副本 SHA-256 一致。

`tuba-kafka-observer` 使用独立 SCRAM-SHA-512 凭据，只有 cluster Describe、`tuba.` Topic Describe、`tuba-` consumer group Read/Describe。Exporter 与告警按当前 Zeek profile group ID 过滤，排除历史 placeholder group。2026-09-28 现场发现 `/opt/tuba/collector-live/manage_zeek_live_pipeline.py status` 显示 `tuba-source-adapter`、`tuba-normalizer` 为 stopped/stale；conn source consumer group 最新读数 lag 11,199、无活动成员，其他来源 Topic 也有积压。Prometheus 已出现 lag 与 inactive-with-backlog 告警；此次部署没有重启业务数据面，也没有修改/重置 offset。该数据面恢复是当前需处理的运行事项。

Grafana admin 口令随机生成并保存在 `/opt/tuba/monitoring/secrets.json`（root-only）；使用本机 SSH 隧道访问，口令可由主机管理员从该文件读取。Alertmanager 外发通知未部署，因为尚无指定渠道及接收端 URL/凭据。Prometheus UI 告警和 Grafana dashboard 已可本地查询，外部触达仍未闭环，O05 保持开放。

### O05 数据面积压恢复跟进（2026-09-28）

在 248 首次核对时，`tuba-source-adapter` 与 `tuba-normalizer` 为 stopped/stale。已将 `start-components` 增加到数据面管理器，并在 248 部署后仅恢复这两个组件。恢复时从仍在运行的 `tuba-ingest` 进程读取原有 `SOURCE_ADAPTER_TOKEN`，复用 consumer group suffix；`tuba-ingest`、raw/quarantine/standard indexer 均未重启，Kafka offset 未重置。恢复后六个数据面进程均报告 running。

恢复后的 Prometheus 样本显示 lag 仍高：Normalizer `raw.live2` 消费组在两次约 42 秒间隔采样中从 4,735 上升到 8,009；source-adapter 消费组亦有持续积压。由此确认进程恢复成功，但处理速率尚未追上输入或积压仍在回放；本记录不将 Kafka lag 记为已恢复。下一步应比较每个来源 Topic 的 LOG-END-OFFSET 与 CURRENT-OFFSET 增速，并核对 ingest receipt 与 Raw ES bulk 写入吞吐/错误，再决定限流或容量调整；不得通过重置 offset 清积压。

### O05 外部邮件告警状态（2026-09-28）

接收地址确定为 `1096429536@qq.com`。QQ SMTP 客户端授权码仍待提供；应使用 `smtp.qq.com:465` 和客户端授权码，不接受网页登录密码。Alertmanager receiver、凭据文件与受控测试邮件尚未配置/发送，因此告警通知闭环未完成。

### O05 消费积压恢复与进程监督（2026-09-28）

进一步排查到 source-adapter 与 standard-indexer 会在 Kafka 暂时关闭连接/提交 offset 失败时退出；broker 进程持续运行，故障路径是 worker 返回错误后没有本地自动恢复。已扩展 `/opt/tuba/collector-live/manage_zeek_live_pipeline.py`：单组件恢复及 `--restart start-components ...` 会沿用同一 adapter token 和消费组，通过进程监督器在异常退出 2 秒后重启。六个组件（ingest、source-adapter、raw-indexer、normalizer、quarantine-indexer、standard-indexer）均已迁入监督器；执行时只逐个优雅重启指定 worker，未改 Topic、ACL 或 offset。

积压回放期间，Prometheus consumer lag 峰值约 15,596；source-adapter 在约 119 秒内增加 5,479 条成功回执计数，Normalizer 消费组 lag 在 1 分钟采样中下降约 3,224，Raw-indexer lag=0。随后观测六个进程均为 running，source-adapter `/health/ready` 返回 ready，Kafka exporter 聚合 lag 先为 0、最新为 4 条在途记录。此次不执行 offset reset；少量非零 lag 是采样时仍在处理的新消息，不视为丢失。需在常态运行中继续监视监督器重启次数与 consumer lag。

### 可靠性跟进复核（2026-09-28）

本机代码验证新增了两组故障恢复回归：`internal/sourceadapter/adapter_test.go` 注入一次 Kafka Fetch 失败与一次 offset commit 失败，验证同一事件只调用一次 ingest receipt、重试后再提交 offset；`internal/ingest/server_test.go` 覆盖 Beat Topic 来源绑定、可信上下文、receipt 重复幂等、同 ID 不同正文冲突、无效 token 与未绑定 Topic。`go test ./...`、`go vet ./...`、`git diff --check` 均通过。

248 现场只读复核：Zeek 数据面管理器报告 ingest、source-adapter、raw-indexer、normalizer、quarantine-indexer、standard-indexer 六项均 running；source-adapter `/health/ready` HTTP 200；Prometheus `sum(up)=6`，Kafka consumer lag 聚合值为 0。此前已在 source-adapter 子进程上做受控 SIGKILL，监督器自动拉起新 PID 且 readiness 恢复。没有重置 offset，也没有停止 Kafka、PostgreSQL 或 Elasticsearch。

以上只关闭“单 worker 异常退出恢复”和当前静态积压的验证切片，不代表 COL-07/V02 总体通过。仍待在隔离环境按矩阵覆盖数据面整机重启、网络分区及长时间断连、Collector 本地队列/归档 spool 上限与清理、磁盘满、日志轮转覆盖、Kafka/PG 故障恢复、配置/凭据切换、跨 offset 重复和 Topic 重建；每项需记录确认水位、数据缺口及回放证据。邮件告警仍按用户要求暂缓。

### 21 归档 stage 清理安全复核（2026-09-29）

2026-09-29 在 21 只读确认 TUBA `archive-sync` 与 conn/dns/http/ssl 四个 Filebeat 均 running。`archive/` 文件元数据占用 145,021,139 B，`data/` 目录占用 41,715,613 B。现场脚本当前没有 stage 自动清理，未重启上述进程。读取 registry state 的只读汇总显示 conn 有 7 个、dns 有 5 个归档 stage 的 offset 低于解压文件长度，另有 3 个 dns stage 文件未在可读取快照中匹配；http/ssl 没有同格式的数值快照。该汇总不能证明这些文件是否已被输出端确认，因此只记录为需要调查的缺口，不视为精确 ack 指标。

结论：单凭 stage mtime 超过 6 小时不能安全删除；Filebeat 停机、缓慢读取或输出积压时会丢失尚未确认的数据。未将清理脚本部署到 21。本地归档管理器当前要求显式的 acknowledged stage 集合才会删除过期文件，默认 fail-closed；新增标准库单元测试验证未确认保留、显式确认后删除、成功解压重复跳过、坏 gzip 清理，以及模拟 ENOSPC 时保留源 gzip 并移除 `.partial`。4 项测试在 Windows 工作站与 21 的 Python 3.8.10 临时目录均通过。自动清理仍待接入可信、逐文件且可验证的 Filebeat 输出确认状态，并在隔离流量上演练断网超时、恢复和积压排空后再上线。

### Filebeat 磁盘队列强杀恢复演练（2026-09-29）

在 21 上运行 `scripts/verify_filebeat_diskqueue_recovery.py` 的隔离副本，建立独立 Filebeat 配置、registry/data 目录和临时 Kafka 容器；仅监听 loopback，并使用专用临时 Topic。测试先将 Kafka 输出指向不可达端口，产生 2,000 条带唯一 ID 的合成 Zeek 风格事件，确认持久磁盘队列约 1,919,784 B；随后强制结束测试 Filebeat，再以同一配置和队列状态恢复到临时 Kafka。

验收结果：Filebeat output acked=2,000；Kafka 消费核对 unique=2,000、duplicates=0、malformed=0。演练通过，证明当前 8.19.0 Linux Filebeat 的磁盘队列可在进程 SIGKILL 后恢复并完成输出。测试容器、临时配置/registry/queue、传输 tar 与临时镜像均已清理，21 的生产 Filebeat 配置、进程与 offsets 未更改。

后续将同一脚本扩展为队列接近上限的隔离测试：50,000 条事件、单事件 padding 1,024 B，Kafka 离线时等 16 MiB 队列达到至少 12 MiB 再 SIGKILL。实际队列文件为 15,999,234 B（约 15.3 MiB）；用相同临时配置/registry/data 恢复后 output acked=50,000，Kafka 消费端 unique=50,000、duplicates=0、malformed=0。队列接近上限时 Filebeat 未退出；恢复后成功继续读取原始输入中尚未入队的事件。

再增加真实 Kafka broker 停止/启动场景，发送 100,000 条事件。先确认部分事件已到临时 Kafka，再停止 broker；queue 达 15,999,360 B 后 SIGKILL Filebeat，启动原 Kafka 数据目录及 Filebeat。最终 100,000 个唯一事件齐全，但重复 15 条、malformed=0。说明 Filebeat/Kafka 故障窗口内是 at-least-once，Kafka 已收但 Filebeat 本地队列 ACK 未持久化的记录会重发。现在的 Raw ID 依据 Kafka topic/partition/offset，因此重复落在新 Kafka offset 会被当作新 Raw 记录，并派生新的 UIM event ID；系统尚未做跨 offset 去重。脚本输出将明确标记 `PASS_AT_LEAST_ONCE_DUPLICATES_OBSERVED`，不能误读为无重复。

三次 broker 失败演练的重复数分别为 15、94、65。重复记录的 `log.file.device_id`、`log.file.inode`、`log.offset`、消息 SHA-256、`@timestamp` 与 agent 元数据完全一致，仅 Kafka delivery offset 不同。这确认队列和输入续读没有丢失唯一事件，但跨 Kafka offset 会重复投递，Filebeat/Kafka 这段提供 at-least-once 而非 exactly-once。

本地已实现稳定 Filebeat 源位置 `filebeat-v1:<device_id>:<inode>:<log.offset>`，并将 `kafka-v1:<topic>:<partition>:<offset>` 独立存为 `delivery_position`。Zeek 事件缺少文件身份或 offset 时拒绝接入并保留 Kafka offset；receipt、Raw ID 和 adapter 校验使用稳定源位置，同一源行跨 Kafka offset 重发应复用原 Raw receipt。Raw strict mapping 的兼容字段由 `scripts/migrate_raw_delivery_position_mapping.py` 一次性扩展，默认 dry-run，apply 后读回核验。相关 Go 测试、`go vet`、迁移脚本及 collector 管理器 Python 测试、py_compile、`git diff --check` 均通过。

此实现目前仅在工作区，尚未部署到 248，也未对现存 Raw index 执行 mapping migration；没有完成真实 source-adapter→PG receipt→Raw ES→DIP/UIM ES 的端到端重复对账。部署前必须排空 adapter lag 并优雅停止消费、迁移现存 Raw mappings，再部署新二进制并隔离验证 Raw/domain 唯一数。临时测试容器与数据目录已由 runner 清理；本轮在 21 的测试 tar/脚本副本/日志及未使用测试镜像、71 的临时传输 tar、工作站测试 tar 均已按精确路径清理，71 上预存测试镜像保留。

这些测试仍不证明宿主机掉电/整机重启、文件系统真正满时的恢复、生产归档 stage 的安全回收。生产 spool 仍无可靠 per-file ack 来源，所以不能仅以 TTL 删除归档 stage。剩余 COL-07 重点是空间耗尽保护、宿主机重启恢复、源日志轮转与归档积压回放、跨 offset 重复的线上代码验收，以及从来源 offset 到 ES 的逐段数量和缺口对账。

### 248 数据面当前抽查（2026-09-29）

只读复核 `manage_zeek_live_pipeline.py status` 显示 ingest、normalizer、quarantine-indexer、raw-indexer、source-adapter、standard-indexer 六项均 running，Prometheus `/-/healthy` 返回健康。Prometheus 的连续两次 `kafka_consumergroup_lag > 0` 查询分别看到 TLS=1、随后 network=9；当前事件流仍在到达，lag 是变化中的瞬时值，不能据此宣称持续积压或 lag=0。没有改 offset、重启 worker 或修改配置。后续验收需按时间序列观察 lag 是否持续增长，并把 Raw/indexer/ES 新鲜度纳入同一组对账。

同日后续只读复核：六个数据面组件仍为 running，capacity guard readiness=`ready/normal`，Prometheus healthy；Prometheus 瞬时 `sum(kafka_consumergroup_lag)=0`、`sum(up)=6`。这只说明该采样时刻监控覆盖的目标均 UP、被纳入的消费组无积压，不替代持续稳定性窗口与 source→receipt→Raw→DIP/UIM→ES 数量/新鲜度对账；本次未重启或改动服务、配置、Topic 或 offsets。

### 后续可靠性部署与 Windows shadow（2026-09-29）

本节后续结果更新本文件上面写有“未部署清理逻辑”“只在工作区”的早期状态；早期试验和观察数据仍作为当时的历史证据保留。

- **21 Filebeat 归档确认清理**：工作站脚本解析 Filebeat `registry/filebeat/active.dat`、active snapshot 与 WAL 中的 set/remove 操作，并在读取前后检查文件元数据一致。只有 `meta.source` 指向的 stage 文件且持久 `cursor.offset >= file size` 时才视为确认；registry 不完整、不可读、格式未知或期间变化一律 fail-closed。6 项管理器测试通过。先在 21 临时脚本只读运行 `archive-status`：conn/dns/http/ssl 各 36 个过期文件。生产替换脚本前备份原版、SHA-256 核对并 `py_compile`；只替换 `archive-sync` 子进程，conn/dns/http/ssl 四个 Filebeat 均未重启。首次安全清理仅移除已确认的 conn 18 + dns 18 文件；余下 18 + 18 + 36 + 36 = 108 个，约 16.8 MB，继续等待 registry 确认。恢复后状态显示四个 Filebeat 和新的 archive-sync 均 running。
- **248 稳定位置/跨 offset 幂等准备**：新增 Filebeat 稳定位置 `filebeat-v1:<device_id>:<inode>:<log.offset>`，Winlogbeat 稳定位置 `winlogbeat-v1:<hex(computer_name)>:<hex(channel)>:<record_id>:<UTC timestamp>`。原 `topic/partition/offset` 另存 delivery position；Windows Security 缺坐标会被拒绝。Raw ES 以严格映射兼容历史版本：delivery position 保存在 PG receipt 与 Raw Kafka envelope，Raw ES 文档序列化时省略该可选字段，不需要管理员迁移既有映射。Go 全量测试、`go vet ./...`、Python archive tests/py_compile 均通过。
- **248 运行版本滚动**：构建并核验 Linux amd64 `tuba-ingest`、`tuba-source-adapter`、`tuba-raw-indexer`、`tuba-normalizer` 四个 SHA-256 后部署；覆盖前备份旧二进制至 `/opt/tuba/backups/reliability-20260929/`。通过 `--restart start-components` 逐项优雅重启，保留 adapter token、context、group suffix 与 offsets。读回六个组件 running、ingest `/health/ready` 为 200、Prometheus 6/6 target UP；source-adapter 的 offset commit counter 持续增加。两轮各 5 次、间隔 30 秒的 lag 样本总量在 0–10 间波动，最终读数均为 0；短时非零分布在标准 DNS/network、conn source-adapter 与 Raw-indexer。此窗口未显示持续单向增长，但还不能替代更长的稳定性、新鲜度趋势及真实跨 offset 重放统计。
- **Windows Security shadow**：从官方 Winlogbeat 8.19.0 官方 SHA-512 核验包在 139/169 各运行临时 shadow 配置，限定 Security Event ID、`ignore_older:24h`、`include_xml:true`，仅输出至远端专用临时目录，不连 Kafka、不注册 Windows service；逐机采集后清理目录。两主机均 `test config` 与实读成功：139 为 1,762 条（4719 1,728、4776 34），169 为 111 条（4776 111），shadow 输出事件均可见原生 XML 字段。正式 Winlogbeat Kafka 接入仍需通过现有授权 API 建立各自的来源 context/release、SCRAM 用户与 exact ACL，并更新 adapter allowlist；没有使用 SQL 伪造用户或审计事件。
- **未完成/安全边界**：SMTP 邮件告警依用户明确要求暂缓。尚未完成 COL-07/V02 的 Topic 重建、目标机掉电/重启、生产源日志覆盖、磁盘满整机恢复、积压换凭据、实流跨 offset 的唯一 Raw/domain 对账以及端到端容量恢复速率。没有指定异机备份目的地，也没有可执行的 B04/V08 restore 验收目标；所以单节点磁盘/整机丢失仍无可承诺的 RPO/RTO。Windows source registration 需要 `source:manage` 有效操作者会话，当前没有可用令牌，正式 Collector 未启动。
