> 历史设计与验收证据：2026-09-27 已被成熟采集器＋TUBA 管理方案替代，不再作为实施要求。当前设计见 [Collector 设计](../COLLECTOR-DESIGN.md)。

# Collector 重新设计与实施方案

日期：2026-09-26。状态：设计定版，代码按 COL-01 至 COL-07 分片推进。范围：product 的新接入链路。本地代码交付不表示已安装或验证任何服务器上的采集程序。

配套：[总体流程图](../diagrams/collector.html) · [完整目标架构](../TARGET-ARCHITECTURE.md) · [实施 TODO](../IMPLEMENTATION-TODO.md)

图中的 Kafka → 接收 HTTP 202 表示确认依赖；实际响应由 ingest 通过原 HTTP 连接返回，Collector 不连接 Kafka。

## 1. 决策与边界

采用 Go 实现独立的 `tuba-collector`，发布 Windows/Linux 二进制。每台来源主机一个服务进程，可配置多个来源；各来源独立游标、队列配额、发送状态和凭证。首期不依赖本地 Kafka、Redis 或其他服务。

首期自研的理由：现有 ingest 要求每条原始记录携带稳定位置、逐条确认和可信来源绑定，需要把来源游标与落盘操作放在同一个事务中。选 Go 复用产品技术栈；Windows 使用系统 Event Log API，文件使用 OS 文件身份。已有 Winlogbeat/Fluent Bit/Vector 的来源可作为后续适配渠道，不能未经验证就把 HTTP 成功等同于来源游标安全。

| 组件 | 职责 |
| --- | --- |
| 来源适配器 | 读取原始记录、提供可恢复游标和稳定来源位置，识别日志代次与缺口 |
| 本地存储 | 原文、请求正文、游标、发送状态、失败原因的持久化 |
| 发送调度器 | 公平调度、并发限制、背压、HTTP 投递、重试与回执校验 |
| 配置与身份模块 | 校验来源绑定、保存不可变来源上下文、替换凭证、受控配置切换 |
| 状态模块 | 本地健康检查、结构化日志、指标和来源心跳 |
| tuba-ingest | 凭证授权、可信信封、稳定 raw ID、回执及 Kafka 持久确认 |
| tuba-normalizer | DIP 厂商语义解析、UIM 校验分类、标准事件与平台隔离输出 |

Collector 可以把 Windows XML 转成保留原生字段的 JSON，完成字符解码和消息边界识别。user/user.target/group 角色解释、事件领域、质量判断和 ES 路由由 DIP/UIM 执行。过滤引擎与首版 Collector 一起实现；生产丢弃规则先观察和验证，再小范围、分批启用。

### 1.1 过滤能力首版交付，生产规则分阶段生效

巨大日志量意味着过滤不能作为事后优化：等全部记录进入 SQLite、HTTP 和 Raw Kafka 后再过滤，已经承担了多数采集成本。首版就需要三层策略接口，并明确各自作用：

| 过滤层 | 执行位置 | 能减少什么 | 首期动作 |
| --- | --- | --- | --- |
| 来源选择 | Windows 原生查询 / Zeek dataset 与文件选择 | 源端读取、解析、磁盘、网络和平台成本 | 作为最主要的减量入口；只选已登记、业务需要的数据源 |
| Collector 记录规则 | 适配器读取一条后、SQLite 入队前 | 本地队列、网络及接入成本 | 同首版提供版本化确定性规则、影子统计及保留/丢弃动作 |
| 平台语义规则 | Raw 接收后，经 DIP/UIM 后 | 标准索引和分析成本 | 决定标准事件去向、隔离与分析用途；无法挽回已消耗的采集和传输成本 |

策略采用受限、确定性的表达：数据集/频道选择、字段存在/相等/集合包含/数值或时间区间等有界条件。禁止首期任意脚本、用户正则和无法估算成本的查询。默认动作必须是保留；策略版本、来源绑定、字段字典、优先级、原因码及依赖它的检测场景随不可变 source_context 固定。规则编译和成本检查在配置发布前完成，运行时不能因规则加载失败退化为“全部丢弃”。

过滤动作只有：

- **保留**：在本地事务中保存原文、固定请求正文、规则版本及读取游标。
- **丢弃并计数**：不保存单条正文；在同一个本地事务中写入按来源/规则/原因/时间窗聚合的计数并推进读取游标。崩溃后不会重复计数或静默跳过。
- **抽样保留**：留作后续能力，仅对经检测负责人确认可抽样的场景开放。使用稳定 source_position 确定性抽样，规则版本与样本率进入审计和分析元数据；身份、安全审计和计数类规则默认禁止抽样。

过滤决策拆成“什么时候实现机制”和“什么时候生效规则”：

1. **首版实现机制。** adapter 的来源选择、规则版本、确定性求值、shadow 模式、过滤计数和事务游标全部纳入 COL-02/COL-04/COL-05。初始配置只选择明确需要的 channel/dataset，不导入猜测出来的 EventID 丢弃表。
2. **先用小范围真实流量观察。** 在验收主机和高峰时段运行 shadow：规则计算并统计命中，但继续保留和投递记录，不造成数据损失。由于 shadow 仍发送全部日志，需限定主机和观察窗口，并预留容量；不可在所有高流量主机上无限期影子运行。
3. **审核影响再灰度执行。** 展示命中量、预计磁盘/网络节省、事件样例（限额且脱敏）以及受影响场景。检测输入必需的事件成为保护集合。规则先在一台主机或一个 dataset 强制执行，监控 ingest 量、UIM 结果、检测新鲜度和命中原因，再扩大范围。
4. **版本回滚和停止读取都必须安全。** 过滤版本回滚从下一条尚未读取的记录生效；已入队事件保留原版本，不重解释。磁盘达到硬保护线时暂停来源读取并报告源端覆盖风险，不自动增加丢弃规则来隐藏压力。

过滤指标包括每来源读取量、保留量、规则丢弃量、源端查询排除估算、版本和命中原因；不为每条丢弃记录单独写日志。场景覆盖检查与影子统计在 COL-06/COL-07 验收。只有在真实流量足以代表业务峰值及周期后才启用生产规则；观察周期由来源使用模式决定，必须覆盖高峰和相关业务周期，不用固定天数替代证据。

## 2. 部署与来源模型

拟部署拓扑，不代表本轮已执行：

| 主机/角色 | Collector 配置 |
| --- | --- |
| Windows 域控 10.6.6.139 | 统一 ZIP 包 + Collector 自管理 CLI；目标是本地 Security 频道，Windows Event Log adapter 尚待实现 |
| Windows 成员机 10.6.6.169 | 如纳入采集范围，使用相同 ZIP 包、配置格式和控制命令，单独来源身份 |
| Zeek 10.6.69.21 | 同一 ZIP 包中的 Linux 二进制，读取 Zeek JSON 日志；由 Collector 自己管理启动和停止 |
| 平台 10.6.68.248 | ingest、Kafka、标准化和索引服务 |

首期按主机本地采集，避免由 248 用管理口令周期性远程拉 Windows 日志。WEF/WEC 汇聚采集可后续增加；届时必须保留原始发出主机与频道身份，不能只用 WEC 主机生成来源位置。

三层标识：

- collector_id：一次安装的稳定 ID，保存在受保护的数据目录。重启和升级不变化，克隆安装不能复用。
- source_instance_id：平台登记的逻辑来源，绑定组织、namespace、vendor、dataset。一个 Zeek 主机的 conn/dns/http/ssl 分别登记来源，匹配当前单来源单 dataset 模型。
- stream_id：来源内部的独立读取流，例如 Windows 频道或 Zeek 文件流。source_position 内包含 stream_id，避免不同流位置冲突。

一个物理来源首期只允许一个活动读取者；本机以服务单实例锁与数据库锁防止重复运行。数据目录只能位于本地磁盘，禁止多个进程或节点共享 SQLite 文件。

## 3. 正常处理流程

1. 启动时加载本地配置和凭证引用，打开数据库，恢复来源游标、未确认记录和不可变来源上下文。
2. 适配器从已落盘的读取游标继续读取，构造稳定 source_position，保留原始证据，并把发送正文序列化一次。
3. 在同一个本地事务中插入队列记录并更新读取游标；提交成功后才能继续越过该记录读取。事务失败则不推进游标。
4. 调度器领取 pending 记录，写入有时限的 inflight 租约，以固定正文、固定位置和固定上下文发起 HTTP 请求。
5. ingest 验证来源凭证和上下文，保存首次接收元数据，构造 Raw 信封，等待 Kafka 确认。
6. Collector 收到匹配本次记录的 202 回执后，本地事务保存回执并标记 acked，更新连续投递水位及乱序确认范围。
7. 清理已 acked 的 payload，保留必要进度和回执摘要。Kafka 的 Raw 归档与 DIP/UIM 分支继续独立处理。

202 表示 Kafka 已确认原始消息，不表示已写入 ES，也不表示 UIM 合格。页面应分开显示“采集已落盘”“平台已接收”“标准化/索引结果”。

## 4. 两种游标与崩溃恢复

| 状态 | 何时更新 | 恢复含义 |
| --- | --- | --- |
| read_cursor | 原始记录和发送正文在本地事务落盘时 | 下次从这里继续读取；此前未确认数据已经在本地 |
| ack_watermark + ack_ranges | 收到并校验服务端回执，写入本地事务时 | 已确认投递的连续水位和乱序确认区间 |

read_cursor 可以领先于 ack_watermark；两者之间的记录必须存在于持久队列或本地隔离区。永久拒绝不能伪造 ack，也不能让连续投递水位跨过该缺口；后续成功记录可用范围压缩保存，避免单条坏记录阻塞全部来源。

适配器使用本地唯一键 `(source_instance_id, stream_id, source_position)` 防止崩溃后重复入队。同一位置、同一正文复用原队列项；同一位置正文不同必须报告冲突。

| 崩溃时点 | 恢复行为 |
| --- | --- |
| 已读出但本地事务未提交 | 原游标重读 |
| 本地事务已提交但尚未发送 | 从队列发送，无需依赖原文件仍存在 |
| Kafka 已接收但 HTTP 响应丢失 | 原记录重发，raw_event_id 不变 |
| 收到 202 但本地 ack 事务未提交 | 重发；允许消息重复 |
| ack 已提交但 payload 未清理 | 启动后安全清理 |

主机磁盘损坏无法由单机 spool 保证恢复；在源日志保留期内可受控重采，超出范围必须生成数据缺口记录。禁止把“进程崩溃恢复”描述为主机级容灾。

## 5. 本地持久队列

首期采用 SQLite WAL，单写入者、同步提交，`synchronous=FULL`。适配器通过有界内存通道提交，累计最多 100 条或 100 ms 形成一次事务；这两个值是初始参数，吞吐验收后调整。事务提交前的记录没有推进 read_cursor。

建议逻辑表：

| 表 | 关键内容 |
| --- | --- |
| collector_meta | 安装 ID、数据库版本、配置版本 |
| source_contexts | 来源绑定、不可变 context ID、epoch、release、payload 格式版本 |
| streams | 本地/原生代次、读取游标、ack 水位、配置版本、状态 |
| queue_items | 本地序号、source_position、context ID、固定正文、正文 hash、原文、状态、重试信息 |
| ack_ranges | 已确认的序号区间；可合并连续范围 |
| rejected_items | 永久错误引用、诊断、原始记录、人工处置与重投记录 |
| source_gaps | 来源日志被覆盖、文件消失、游标失效、明确缺失范围及处置记录 |

状态：pending → inflight → acked；暂时错误回到 pending；永久错误进入 rejected。inflight 不是成功状态，重启后租约到期的项回 pending。acked 状态与回执必须在同一事务中持久化。

正文在首次入队时固定；重试不得重新添加当前时间、重新格式化 JSON、重新解析 XML或更新 release。源码日志和发送正文不是同一字节表示时，两者及各自 hash 都保留，明确“原文 hash”与“传输正文 hash”。

SQLite、WAL、隔离数据、索引及临时空间共同计入磁盘预算；低空间时不执行需要复制整个数据库的 VACUUM。日志轮转不会删除队列中的未确认记录。

## 6. Windows 适配器

使用 Windows Event Log 原生 API，启动时按 bookmark 恢复读取并继续订阅；周期性拉取补齐通知丢失。首期采集 Security 的原生事件，不用 EventID 白名单假装已经完整覆盖；如配置频道查询过滤，必须展示过滤条件及版本。

稳定位置格式示例：

`win:<stream_id>:<channel_generation>:<EventRecordID>`

channel_generation 保存在本地数据库，标识一次频道日志代次。清空日志、RecordID 回退、bookmark 失效或查询范围变化，需要先判断“日志重置”还是“旧日志已覆盖”，然后事务性记录新代次或 gap。不能仅看到一个较小 RecordID 就在每次重启时创建新代次。EventID 是事件类型，不可用作位置键。

输出适配当前 DIP 的 `@timestamp` 与 `winlog.provider_name/channel/event_id/computer_name/event_data` 等字段；保留原始 XML、原生时间和 RecordID。字段提取只是读取格式转换，操作者/目标账号等语义映射仍由 DIP 执行。重复名称和无法转换字段不能悄悄覆盖，应保留原始表达。

首次安装必须显式配置 initial_position：oldest_available、now 或指定时间；now 的跳过范围需要审计。默认 oldest_available。若 bookmark 指向已被覆盖日志，进入 gap_detected，报告最后确认位置、当前可读最早位置；经配置的恢复策略继续，绝不自动宣称连续无缺口。

来源读取权限采用专用服务身份及对应频道读取权限。部署验收必须实际验证 Security 访问授权，不能把域管理员口令放进 Collector 配置。

## 7. Zeek 文件适配器

首期要求 Zeek 输出 JSON Lines，读取 conn.log、dns.log、http.log、ssl.log 和配置的轮转目录。启动扫描与持续文件通知结合；通知只用于唤醒，实际位置以持久游标为准。

稳定位置格式示例：

`file:<stream_id>:<file_generation>:<byte_offset>`

文件身份由设备/文件 ID（Linux device+inode）、持久登记的文件代次、前缀指纹和路径历史共同识别；路径不是文件身份。byte_offset 指一条完整记录的起始字节；读取游标存记录结束位置。只有完整换行记录才能提交，末尾半条留待下一次读取；封闭文件存在未结束记录时按受控策略隔离处理。

rename 轮转：持有旧文件句柄，读完旧文件；新文件登记新代次。copytruncate：发现长度小于游标时建立新代次并记录潜在缺口；该模式无法保证截断竞态下不丢日志，部署要求采用 rename。inode 复用且无法证明连续性时暂停该流并报告，而不是冒险把新文件当旧文件。

原 JSON 行可直接作为请求正文，重试保持字节不变。首期不自动注入 _path 或做字段重命名；数据集由登记的来源绑定，DIP 已能根据 dataset 解析。原始行分隔符和文件编码属于原文存储元数据。

暂停采集期间，Zeek 自身仍会写日志。必须配置源端保留期覆盖预计离线时间和追赶时间；“本地停止读取”不能被描述为能让 Zeek 停止产生日志。

## 8. HTTP、确认与错误处理

当前接口继续作为首期投递入口：

```http
POST /api/v1/ingest/events
X-API-Key: <来源凭证>
X-Source-Position: <稳定位置>
X-Source-Context: <不可变上下文 ID，新增合同>
Content-Type: application/json

<落盘时固定的原始 JSON 对象>
```

X-Source-Context 已进入接入合同。它不是认证凭证；服务端用来源凭证与上下文 ID 联合查询冻结的来源绑定。不能把客户端提供的 epoch、release、namespace 直接当可信信息。

当前接口单条正文上限 1 MiB；Collector 必须在入队时检查编码后的完整正文大小。超限记录以分块本地原文文件保留，并事务登记诊断，不能截断正文后按成功发送。未来大对象接口另行设计。

外置大记录必须先写临时文件、同步数据、原子改名并完成必要的目录持久化，再在 SQLite 事务中登记文件引用和推进读取游标。崩溃后无数据库引用的文件作为待回收孤立文件；数据库引用缺失文件是完整性错误，不能按已留存继续读取。

Collector 校验 202 响应的 receipt_id、raw_event_id、status 和时间字段，并按下发的 ID 算法版本核对请求身份；响应不匹配视为协议异常，保留记录并暂停该来源发送。连接、请求和响应读取均设置超时，响应正文有大小限制。

| 结果 | Collector 行为 |
| --- | --- |
| 202 且回执匹配 | 本地事务确认 |
| 超时、连接断开、408、429、5xx | 保留，指数退避并加随机抖动；尊重 Retry-After |
| 401/403 | 暂停该来源发送，报告凭证/授权问题；磁盘有空间时可继续采集 |
| 409 | 同位置不同正文或上下文冲突，隔离并告警；禁止改随机 ID 再提交 |
| 400/413/415/422 | 记录级确定错误进入本地隔离；大量同类错误触发来源熔断 |
| 404/405、TLS 校验失败、未知 2xx | 配置或协议故障，暂停发送并告警 |
| 410（未来 retired context） | 保留原记录，走受控恢复/重放，不自动替换来源上下文 |

本地 rejected 是“未被平台接收的记录”；平台 Quarantine 是“已接收但 DIP/UIM 未通过的记录”，两者分别展示与处置。

首期沿用单条 HTTP：全局最多 16 个并发，每来源最多 4 个，来源间轮询调度；具体可配置。这允许同来源乱序确认，因此队列不能用“收到最大序号”直接推进水位。Kafka 不承诺还原原生读取顺序，下游按事件时间与来源位置处理。

初始退避基数 1 s、上限 60 s，全抖动；服务器 Retry-After 是最小等待约束。持续网络故障不按尝试次数删除事件。连接恢复后逐步提速，避免全部来源同时清空积压。

批量接口是后续性能任务，必须逐条返回状态和回执；不能用整批 HTTP 200 表示每条已持久化。正式容量指标在单条模式基准完成后给出，不能仅凭并发数承诺 Zeek 高峰吞吐。

## 9. 身份、版本与凭证轮换

必须拆分四个概念：

| 字段 | 改变条件 |
| --- | --- |
| 来源凭证 | 安全轮换、撤销 |
| source_epoch | 管理员明确重置来源身份、旧本地状态确实无法恢复 |
| native_generation | Windows 日志清空或文件生命周期变化，由适配器持久识别 |
| source_context_id | 来源绑定/发布版本的不可变快照；配置切换时产生新上下文 |

raw_event_id 继续由可信组织＋source_instance_id＋dataset＋source_epoch＋source_position 确定，不包含重试次数、密钥、发送时间或 release。native_generation 已编码在 source_position 中。

**Collector 首片已修正 RotateSourceCredential。** 常规密钥轮换不改变 raw ID；credentials 子表允许旧 key 重叠 24 小时，便于本地积压排空。紧急撤销通过禁用来源实现，不能借轮换清空队列。

每条队列记录固定 source_context_id。配置切换先在本地事务中确定边界；旧记录继续使用旧上下文，新读取记录使用新上下文。服务端接受经授权的 draining 上下文，禁止按照“当前最新 release”重解释积压记录。

建议新增 GET /api/v1/ingest/source-context，使用来源 key 返回该来源的可用不可变上下文及协议限制；控制面管理员 API 负责发布/撤销。首次安装可由管理员下发包含 context ID 的配置包；已有本地上下文时，控制面暂时不可用不影响落盘。没有可信绑定的新来源只能等待配置。

ingest_receipts 首次冻结可信元数据、context ID 和 received_at，并记录 kafka_acked_at；payload 不复制进 PostgreSQL。Kafka ACK 后更新回执状态，更新成功才返回 202。进程在 Kafka ACK 与 PG 更新之间崩溃仍可能重复发布；明确采用至少一次和下游幂等，不承诺跨 PG/Kafka 恰好一次。

receipt 与上下文清理必须覆盖允许的重试/恢复窗口。首期关闭仅按年龄自动清理，提供容量告警；后续上线清理前必须具备已排空来源上下文的退休状态和拒绝过期重放的合同。禁止删掉首次接收元数据后，让旧记录以新的接收日期进入新的物理索引。

## 10. 配置、容量与运维

配置含 endpoint、collector_id 引用、SQLite/spool 路径、容量预算、全局并发、来源列表。来源项包含 source_instance_id、context 引用、credential_ref、adapter、输入位置、首次读取策略、来源配额。凭证通过 Windows 受保护存储/文件 ACL 或 Linux 0600 文件注入，日志只显示来源 ID。

首期配置由管理员下发，不执行远程脚本。热更新先验证、持久化配置版本，再按来源切换；输入路径、频道过滤或 payload 格式变化须保留旧记录上下文，并记录边界。程序升级停止新读取、完成本地事务、保存队列后退出；安装 ID 和数据目录沿用，数据库迁移必须可恢复。

容量预算按来源设置，避免一个来源挤占全部磁盘。建议预警 80%、暂停该来源新读取 90%、全局紧急保护 95%，降至 70% 后恢复；同时保留宿主机最小空闲量，例如 max(2 GiB, 卷容量 10%)。阈值为初始默认值，不替代现场磁盘规划。

`所需空间 ≈ 每秒事件数 × 平均持久字节数 × 离线秒数 × 余量系数`

例如 500 EPS、每条平均 2 KiB、24 小时离线，正文约 82.4 GiB，按 1.3 余量约 107 GiB；平均字节数应包含原文副本与队列开销，WAL 峰值还需额外预留。不能统一配置 10 GiB 就承诺一天离线。

磁盘满、损坏或写事务失败时暂停读取并告警，保留未确认和隔离项。Windows Event Log 与 Zeek 原文件可能继续轮转，恢复后必须检测缺口。已确认 payload 按清理策略释放；待发送记录不因 TTL 被静默删除。

状态至少包含：读取 EPS、发送 EPS、pending/inflight/rejected 数量、最老待发年龄、spool 使用量、各来源游标、重试原因、授权故障、源日志保留余量、缺口数量、当前 context/程序版本。

健康：live 检查进程；本地 ready 检查队列可写、游标可恢复、已加载配置。平台不可达时可处于 buffering，不能反复重启采集服务。来源状态独立显示 running/buffering/backpressured/auth_blocked/gap_detected/error。

建议新增来源级 POST /api/v1/ingest/heartbeat，使用来源凭证授权，仅上传白名单状态和脱敏诊断。正文里的 collector_id 是观测标识，平台来源身份仍从凭证解析。心跳失败不阻断本地采集和数据投递。首期不在健康接口返回日志原文或密钥。

## 11. 后续多节点

先横向增加“不同来源的 Collector”，每台仍使用独立本地队列。ingest 可以无本地状态扩容，通过 PG 回执和 Kafka 接收统一保证身份。

同一来源的主动/备用接管必须新增来源租约、fencing、游标转移和原文恢复协议；不允许两台机器共享 SQLite 或同时读取同一个来源然后各自修改代次。节点接管不应自动改变来源 epoch。WEC 汇聚、API 拉取和通用 syslog 作为独立 adapter 扩展，不改变持久队列和确认协议。

## 12. 实施顺序和验收

| 编号 | 交付 | 必须满足 |
| --- | --- | --- |
| COL-01 | 来源上下文与轮换改造 | 换 key 不改 epoch；旧队列固定 release；新 header/错误合同同步 OpenAPI |
| COL-02 | Collector 核心与 SQLite 队列 | 读取游标与入队原子提交；租约恢复；配额和磁盘保护 |
| COL-03 | HTTP Sender | 固定正文重试、回执核对、乱序确认、退避和来源隔离 |
| COL-04 | Zeek adapter 与统一部署包 | rename 轮转、重启续读、半条行、inode 复用/截断诊断 |
| COL-05 | Windows adapter 与统一部署包 | bookmark、RecordID 代次、频道清空/覆盖、原文保留；不安装为 Windows Service |
| COL-06 | 配置、心跳与运维制品 | 无明文密钥日志、升级保留数据、监控和容量说明 |
| COL-07 | 故障与端到端验收 | 从源日志至 Raw/DIP/UIM/ES 的证据可追溯及下列场景 |

必须验证：本地事务各边界强杀；响应丢失；Kafka/PG/网络故障；反复 429；磁盘满；权限失效；日志轮转和覆盖；同位置同正文与不同正文；带积压轮换密钥/发布版本；升级恢复；永久拒绝后的后续事件；异常回执；乱序 202；来源上下文退休。

验收以“已落盘原始记录均有待发、已确认或明确失败的可查询状态”为依据。单机离线时长、吞吐和恢复追赶速度必须给出实测负载及磁盘条件。

## 13. 当前代码与本方案的差距

- Collector 核心首片已实现：`cmd/tuba-collector`、`internal/collector`、Zeek JSONL adapter、SQLite WAL 与 HTTP Sender；当前还没有 Windows Event Log adapter。
- ingest 已支持不可变来源上下文合同、凭证重叠轮换、稳定 Raw ID、冻结可信回执元数据及 Kafka ACK 状态；完整故障验收和上下文发布/退休 API 仍待实现。
- 已有 Windows/Zeek 的首批 DIP/UIM 映射，不能据此宣称采集适配器已实现。
- Collector 已有统一 CLI 生命周期命令和 ZIP 双平台包构建脚本；Linux/Windows 当前包内都只有 Zeek JSONL 文件 adapter，Windows Event Log 仍待实现。主机重启自动启动登记尚未实现。
- COL-01 至 COL-07 尚未全部验收；过滤规则审计与管理、永久拒绝处置、上下文退休、心跳/指标、开机启动安装器及端到端部署验收仍在 TODO。
- 2026-09-26 在 Zeek 主机 `10.6.69.21` 的 `/opt/tuba-collector-validation` 完成隔离数据面验收：从 `/opt/zeek/spool/zeek/conn.log` 取 12 条并替换地址/UID 的匿名化 Zeek conn 记录，Collector 读取、SQLite WAL 入队、shadow 计数及 HTTP 202 回执完整通过；12 条均为 acked、已确认 payload 为 0 字节、shadow 命中 6 条。进程重启后回执数仍为 12，未重复发送。停掉本机临时接收端并追加 1 条记录后，队列显示 12 acked + 1 pending（重试 2 次），游标已持久化；接收端恢复后达到 13 acked、pending 为 0、过滤命中累计 7。验收中发现原 SQLite 文件权限为 0644，代码已改为创建及打开时强制 0600，并在 21 复验为 0600。最终 `tuba-collector status` 显示 stopped；临时接收器已停止。验证包、匿名化夹具和 SQLite 证据保留在 21 的该隔离目录。
- `go test ./internal/collector -count=1` 通过，集成用例覆盖 Zeek JSONL → SQLite → HTTP 回执 → payload 清理与 shadow 计数。
- 2026-09-26 完成 21 → 248 Kafka → Elasticsearch 隔离闭环复验：21 上 Collector 将 16 条匿名化 Zeek conn 记录落入 SQLite 后发送到 248 的临时 ingest 实例；PostgreSQL `tuba_collector_validation` 中 16 条 receipt 全部有 Kafka ACK，隔离 topic `tuba.collector.validation.raw.v1` 可读到 16 条。248 上 Raw Indexer 消费该 topic，ES 指标 `tuba_raw_indexer_indexed_total=16`；索引 `tuba-v1-raw-tenant_a-g1-2026.09.26` 和只读 alias `logs-ueba.raw-tenant_a` 均查到 16 个文档，`raw_event_id` 基数为 16。
- 首轮 Raw ES 写入暴露严格 mapping 漏掉可信信封字段 `source_context_id`：16 条消息进入 DLQ，ES 文档数为 0。已在 `internal/sink/raw.go` 补全 keyword mapping，增加回归测试；删除前确认仅有的验证索引为空后重建，再将隔离 topic offset 重置到 0 重放，16 条全部成功索引。首轮失败记录仍保留在共享 DLQ topic 供排障，不影响最终索引结果。
- 验收完成后已停止 248 临时 ingest 和 Raw Indexer；21 上 Collector 与临时 HTTP 接收器均未运行，Zeek 与 Filebeat 仍运行。验证用隔离 PostgreSQL schema、Kafka topic、ES 索引和匿名化证据目录保留，便于复核。验收当时 8081 返回 502；2026-09-26 进一步确认该端口属于 ADMS webserver 并转发到不可用的 `localhost:18081`，不是 TUBA API。新版 TUBA API 现绑定 `127.0.0.1:8788`，经 8443 HTTPS server 代理；无令牌事件查询返回 401，直连 HTTP 端口已拒绝外部访问。247 上 `tuba` realm 缺失，真实 OIDC 成员查询仍未验收。
