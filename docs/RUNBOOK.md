# TUBA 运维手册

> 2026-10-08 身份边界修正：TUBA 使用内置系统账号与服务器会话；247 Keycloak 仅属于外部 Linux 认证/未来日志来源。历史 OIDC 登录记录已被 [系统登录](SYSTEM-LOGIN.md) 替代。

> 当前产品是单节点、单实例、由产品 Launcher/CLI 管理的部署。下列步骤不得使用历史 Helm/Kubernetes 操作替代。
> 命令的确切安装路径以部署 manifest 为准；设计及恢复边界见 [产品详细设计基线](DESIGN-BASELINE.md)。

## 通用排障顺序

1. 确认影响范围：接入、索引、分析、API、身份或前端。
2. 查看 Grafana 的请求率、错误率、Kafka lag、watermark、磁盘水位和 ES bulk 指标。
3. 使用 `tuba-launcher status -manifest <manifest>` 检查组件状态，再访问各组件 loopback readiness；不得只以 PID 存在判定健康。
4. 从 Launcher 配置的私有轮转日志目录读取结构化日志；操作前核对目录权限和剩余空间。
5. 任何处置都记录开始时间、影响租户、命令、结果和后续任务。

## TubaKafkaLag

症状：`kafka_consumergroup_lag` 持续超过 10000。

**先分清这个数是谁的。** 监控只抓一份组白名单（`MONITORED_CONSUMER_GROUPS`，见下），因为**退役代次会留下一批"有已提交 offset、没有成员"的消费组**——它们的 lag 冻结在某个值上永远不动，`TubaKafkaConsumerInactiveWithBacklog`（"有积压且无成员"）会在它们身上常驻触发。2026-09-30 实测：`tuba-normalizer-…-zeeklive20260927`、`tuba-raw-indexer-…-zeeklive20260927` 与三个无后缀的 `tuba-source-adapter-<hash>` 都是这类孤儿，lag 分别冻在 2060 / 1682 / 14176 等值上。**用 `--all-groups` 排查时它们一定会出现，别当成真积压。**

1. 用 `kafka-consumer-groups.sh --describe --group <组>` 确认 **`CONSUMER-ID` / `HOST` 是否为 `-`**。为 `-` 就是无成员：要么是孤儿（对比组名代次后缀），要么是组件真的没起来——后者才是故障。
   - 248 本机实操（2026-09-30）：248 的 Kafka 只监听 `10.6.68.248:29292`（不监听 loopback），本机查组须显式用该地址 + `/opt/tuba/collector-live/kafka/admin.properties`，且 PATH 中无 java，需 `JAVA_HOME=/opt/adms/adms-jdk`。
2. 确认 Lag 属于 `tuba-raw-indexer-*`、`tuba-standard-indexer-*`、`tuba-quarantine-indexer-*`、`tuba-analysis-*` 还是 `tuba-analysis-sink-*`。
3. 检查对应 Launcher 组件的 CPU、内存、重启次数、readiness 和最近错误。
4. 当前单实例版本不通过临时增加消费者处理故障；若 ES 正常，先确认组件存活、凭据、offset、批量预算和限流状态。
5. 若 ES 出现 `429`、bulk 拒绝或高延迟，先保护 ES，不要扩大读取批量。
6. Lag 下降且 watermark 恢复后关闭事件。

### 组白名单：单一事实源 + 生成器（2026-10-09 起）

白名单的唯一手工副本是 `deploy/observability/single-node/monitored-consumer-groups.txt`；改它之后运行 `python3 scripts/generate_monitoring_allowlist.py` 重写三处产物（`scripts/manage_tuba_monitoring.py` 的 `MONITORED_CONSUMER_GROUPS`、`rules/kafka.yml` 的告警 expr、Grafana 面板 JSON），`--check` 只校验不写入（漂移时 exit 1 报名字）。248 现网三处（kafka-exporter `--group.filter`、prometheus rules、grafana json）已于 2026-10-09 实测与该事实源逐字一致；改了名单要同步 248 时，kafka-exporter 走 `tuba-launcher restart --manifest /etc/tuba/tuba-monitoring.json --service kafka-exporter`，prometheus rules 改后需 reload，grafana 面板 JSON 热加载。**历史教训**：漏改一处不会报错，只会静默不监控——2026-09-30 之前整个 tenant_a（Windows Security）链路不在名单里，当前量最大的接入路径完全没有 lag 监控。

另：`/etc/tuba/kafka-client.properties`（M1 遗留，SCRAM-SHA-256 与 broker 的 SCRAM-SHA-512 不匹配、无任何消费方）已于 2026-10-09 退役为 `.retired`（0600 root 留档）。

来源适配器的组名用 `tuba-source-adapter-[0-9a-f]{16}-<代次后缀>` 这一模式匹配，比逐个列 hash 更耐用（新注册来源自动覆盖）；但**退役代次的无后缀同名组要显式排除**，否则会立刻误报。

## TubaIndexerFailures

1. 查询 DLQ topic，按 `failure.code` 和来源 topic 聚合。
2. 修复 mapping、文档字段或服务逻辑。
3. 使用原 topic、partition、offset 和 payload 执行受控重放。
4. 重放使用稳定 event ID，确认 ES 无重复文档。
5. 将不可恢复数据保留在 DLQ 并记录数据质量缺陷。

## TubaAnalysisStale

1. 查询 `/api/v1/operations/status` 的 `analysis.runtime`。
2. 检查 worker heartbeat、processed/emitted 和 checkpoint watermark。
3. 检查 PostgreSQL `analysis_runs`、`analysis_checkpoints` 是否存在持续错误。
4. 若 worker 卡在单条消息，查看 DLQ；确认坏消息隔离后重启 worker。
5. 重启会从 PostgreSQL processor state 和 Kafka committed offset 恢复。

## Elasticsearch 不可用

1. 保持 ingest 继续接收，Kafka 作为耐久缓冲。
2. 检查集群健康、磁盘水位、分片未分配和协调节点。
3. 不要提交 indexer offset；确认 Kafka lag 增长但无数据丢失。
4. ES 恢复后确认 bulk 重试和 lag 自动追平。
5. 若索引损坏，先从最近 snapshot 恢复，再从 Kafka 补写缺失窗口。

## PostgreSQL 不可用

1. API 写入和分析 checkpoint 会失败，但 Kafka 事件不应丢失。
2. 检查连接数、锁、磁盘、复制延迟和主节点状态。
3. 切换受管主备后滚动重启 API 和 analysis worker。
4. 使用最近备份验证恢复点，并在隔离实例执行恢复演练。
5. 完成切换后检查 membership、案件版本和分析 checkpoint。

## 系统登录故障

1. 使用 HTTPS 入口，检查 API readiness、PostgreSQL 连接及 migration 00022 是否已应用。
2. 核对 `/etc/tuba/auth.json` 的 public_origin 与访问来源完全一致，以及 Secure Cookie、网关路径和系统时间；浏览器写请求必须携带匹配的 Origin。
3. 检查账号是否停用、membership 是否有效、会话是否过期。连续 5 次错密锁定 15 分钟，应等待锁定到期；不要通过 SQL 绕过授权。
4. 退出撤销当前会话，改密撤销全部会话；恢复后验证登录、刷新、退出、改密及管理员/分析师权限。
5. 247 不属于登录依赖；管理员引导、账号迁移和审计见 [系统登录](SYSTEM-LOGIN.md)。

## DLQ 重放

1. 冻结 DLQ 消费，导出待重放消息并备份。
2. 修复后先在隔离租户的 topic 重放。
3. 校验 schema、event ID、租户和业务结果。
4. 生产重放后确认 DLQ 减少且源 topic consumer offset 正常。

## 发布回滚

1. Agent/安装器先验签并将新版本安装到独立版本目录，停止旧组件后原子切换 `current`。
2. readiness 在 5 分钟观察窗内失败时，回切上一版本目录；registry/data 目录不得随二进制回滚。
3. 检查 API、ingest、adapter、normalizer、全部 indexer、control/analysis worker 和 analysis sink，并核对 lag 与新鲜度。
4. 数据库采用 expand/migrate/contract，只做前向兼容恢复，禁止直接执行不确定的破坏性 SQL。

## 磁盘水位与容量

A03 的单节点边界：根盘 70% warning、75% critical、80% 停止新增写入；Kafka 各 Topic 保留 12 小时（2026-09-30 由 24 小时下调：全部 topic 动态设 `retention.ms=43200000`，`server.properties` 同步为 `log.retention.hours=12`），ES Raw/domain/Quarantine 最多 7 个 UTC 日分区。

**告警档位与 ES 的分配水位已刻意分开，改动前它们曾被写成同一组数。** 单节点上 `cluster.routing.allocation.disk.watermark.enable_for_single_data_node=true`（默认）会让 **low 水位阻止一切新分片分配**，不只是副本——所以把 A03 的 70% warning 线直接用作 low 水位，等于把"70% 告警"变成了"70% 之后再也建不出新索引"。本节点常态就压在 70%，2026-09-30 实测后果是索引创建与恢复**时好时坏、ES 不报任何错**（诊断见「异机备份与恢复」）。

因此 `scripts/tuba_capacity_guard.py` 现在写入 ES 的是 **low=75% / high=78% / flood=80%**：80% 仍是 A03 的停止写入线，新分片分配停在 75%（A03 的 critical 线）；**70% 只是上报档位**，由容量守卫的 `tuba_capacity_level` 指标和 Grafana 承担，不再由 ES 强制执行。改动后同一磁盘占用（71%）下 4/4 分片恢复成功，改动前是 1–3 个失败。

1. 先分清哪一层在涨。Kafka 与 ES 各有保留期、会自行封顶；**只有 `ingest_receipts` 会无限增长**（每个接入事件一行，约 1.5 KB/行）。2026-09-30 实测：该表 2,511 MB 时为根盘增长主因，写入约 0.5 GB/天。
2. `SELECT pg_size_pretty(pg_total_relation_size('ingest_receipts')), count(*) FROM ingest_receipts;` 确认表状态与最早一行时间。
3. 日常清理由 `scripts/prune_ingest_receipts.sh` 每日 03:17 执行（保留 2 天）。手动核对先不带 `APPLY=true` 试跑，确认待删行数再执行。
4. 保留期不得随意放大：可重投窗口由 Kafka 保留期（12 小时，2026-09-30 由 24 小时下调）与采集端 `ignore_older` 界定，2 天已是其四倍余量；而 receipt 若比窗口年轻，放大窗口会一行都清不掉、磁盘继续涨。
5. `DELETE` 只把页面标为可复用，文件不缩小但**增长停止**。要真正把空间还给文件系统须 `VACUUM FULL`，它取 ACCESS EXCLUSIVE 锁、阻塞接入数十秒（适配器保留 offset 重试，不丢数据），只在明确的维护窗口执行。
6. 清理循环若报 `stopped after N batches with rows still eligible`，先确认待删行是否真的归零；该报错曾是计数把 psql 命令标签算作一行所致。
7. 异机备份**不再占用本机磁盘**：快照直接写进挂在 `/var/lib/elasticsearch/backups` 的 21 仓库（旧版本在本机暂存，实测 4.9 GB，会把占用推过 80% 并让 ES 全索引只读，见「异机备份与恢复」）。脚本以"必须是独立挂载点"作为前置闸门，避免退回旧行为。

## 采集端(Windows Winlogbeat)运维

1. TUBA 实例与既有采集器**必须隔离**：独立 config、`--path.data`、`--path.logs`，且不注册 Windows 服务。同一主机上可能同时存在别产品的 Winlogbeat，不要改它们的服务或配置。
2. 用 `scripts/manage_windows_winlogbeat.ps1 -Action status|start|stop` 管理；`status` 同时列出“其它实例”PID，用于确认隔离成立。
3. 配置由 `scripts/render_windows_winlogbeat_config.py` 从 `deploy/components` 模板渲染。**渲染产物含来源的 Kafka SCRAM 口令，只写入 gitignore 的 `.runtime/`，不得入库。**
4. 采集器主机上的 `winlogbeat.registry` 记录读取书签。**停止请用脚本的 `stop`；强杀会丢失最多一个刷写周期的书签，下次启动重读那段事件。**
5. 重读本身安全（同一记录按位置去重），前提是 payload 字节稳定：来源字段若随读取变化（如 Windows 渲染的任务名），重读会被判为冲突并进 DLQ。`registry_flush: 1s` 用于缩小这个窗口。
6. 来源未产生数据时，先查 `source_instances` 是否 enabled、Kafka 源 Topic 是否有写入、适配器计数器是否增长，最后才查采集器自身。

## 采集端(21 Zeek Filebeat)运维

1. 管理器是 `/opt/tuba/collector-live/filebeat-r2/manage_zeek_filebeat.py`（仓库副本 `scripts/manage_zeek_filebeat.py`），动作 `status|start|stop|test|sync-archives|archive-status`。四个数据集 `conn`/`dns`/`http`/`ssl` 各有独立配置、registry、磁盘队列与 Kafka ACL；另有一个 `archive-sync` 子进程负责把最近归档解压进私有 spool。
2. **可以只启停单个数据集**：`start ssl`、`stop dns`；不带数据集名就是全部。**线上单个数据集故障请只重启它**——这条能力是 2026-09-30 才补上的，在那之前只能停掉再起全部四个，于是**一个数据集的故障会连带停掉另外三个**（当天实际发生过约 4.5 分钟的四数据集停机）。
3. `start` 对已在运行的数据集是**跳过而不是拒绝**，可以安全重复执行；状态文件里若留下已被强杀进程的条目，会被自动丢弃，不再阻塞重启。
4. **不要碰 21 上别产品的 `filebeat.service`**（systemd）。TUBA 的四个实例都在 `filebeat-r2/` 下，用 `--path.data` 指向自己的目录，与它无关。
5. 归档按 Filebeat 的 registry 游标**确认读到 EOF 之后**才回收；`archive-status` 可查当前水位。**不得仅凭"文件超过 N 小时"删除未确认的归档**——未确认就删等于丢数据。注意 `archive-status` 只统计 `.log` stage：`.source.json` 标记永远不可能"被确认"，把它们算进去会让每个数据集恒报 `acknowledged_files: 0` 和一个永不下降的 pending。
6. **`archive-sync` 是长驻进程，替换管理器文件不会影响正在运行的它**——清理或确认逻辑的改动必须重启 `archive-sync`（`stop archive-sync` + `start`）才生效。
7. 归档能否回收取决于 Filebeat 的 registry。**registry 没有快照（缺 `active.dat`）也能被正确读取**（事务日志记录完整状态，Filebeat 自身就是这么加载的）；但 **`active.dat` 存在却读不了时必须视为不一致并停止回收**。2026-09-30 实测：http 一路自 09-27 起从未写快照，修复前它的 stage 永不回收、体积无界增长。
8. 已知浪费（未修，不是正确性问题）：`sync-archives` 把整点归档解压进 archive 目录后，Filebeat 会把**整整一小时的文件重新发布一遍**，每小时每个数据集都如此，使归档数据在 source topic 里翻倍。下游 receipt 去重会吸收（同一记录指纹相同故稳定位置一致，不会产生重复文档），代价只是 adapter/ingest 多处理一遍。详见 TODO 的 2026-09-30 日志轮转记录。

## 新增命名空间的数据面

1. 一个命名空间对应一套数据面：raw-indexer、normalizer、quarantine-indexer、standard-indexer 各一；消费组与 metrics 端口都必须与既有链不冲突。
2. `scripts/manage_tenant_pipeline.py` 负责拉起并监督，命名空间、消费组后缀、metrics 端口基址由环境变量覆盖（默认与 tenant_a 部署一致）。
3. ingest 与 source-adapter 是**共用**的：ingest 按来源命名空间把原始事件路由到对应 raw topic，adapter 已消费全部来源 topic，不需要为新命名空间再起一套。
4. 前置条件：该命名空间的 Source Topic、raw/events/quarantine/dlq Topic 与服务身份 ACL 必须已就绪，否则消费组会静默空转。
5. 启动后核对四个组件的 lag，以及对应 ES alias 是否开始出现文档。

## Launcher 管理数据面（248 现状）

248 的 17 个数据面服务（2026-10-02 起：原 11 + 每命名空间新增 entity-worker/analysis-worker/control-worker 各一）由产品 Launcher 统一管理，**不注册 systemd**。清单 `/etc/tuba/tuba-services.json`（0600 root），密钥 `/etc/tuba/tuba.env`（0600 root），状态与日志分别在 `/var/lib/tuba/launcher` 与 `/var/log/tuba`。Launcher 二进制本身是受管制品：`/opt/tuba/bin/tuba-launcher` 由 `scripts/package_tuba.ps1` 从仓库源码构建（见 `dist/tuba-*-linux-amd64.tar.gz` 内 `bin/tuba-launcher`），同目录有 SHA-256 sidecar `tuba-launcher.sha256`（0640 root），更换后须 `sha256sum -c` 复核；替换磁盘文件不影响已运行的 Launcher 进程，下次重启自然用新版。

```
tuba-launcher status  --manifest /etc/tuba/tuba-services.json
tuba-launcher status  --manifest /etc/tuba/tuba-services.json --service zeek-raw-indexer
tuba-launcher logs    --manifest /etc/tuba/tuba-services.json --service zeek-raw-indexer --tail 100
sudo tuba-launcher restart --manifest /etc/tuba/tuba-services.json
```

**单服务操作**（2026-10-01 起，`--service` 可重复选择多个；不带就是原来的整份清单行为）：

```
tuba-launcher stop    --manifest <清单> --service <名字>     # 停单个服务，其余不动
tuba-launcher start   --manifest <清单> --service <名字>     # 恢复被 stop 的服务
tuba-launcher restart --manifest <清单> --service <名字>     # 只重启该服务（对已停止的服务等价于 start）
```

- `status` 里被 `stop --service` 停掉的服务显示 `stopped` 而 Launcher 仍在运行；这不是故障，是**期望状态**。
- **`stop --service` 是持久期望状态，不是一次性动作**：标记写在 state 目录（`<名字>.stop-request`），服务不会被自动拉起，**跨 Launcher 主进程重启和 248 整机重启都保持停止**（`@reboot tuba-boot` 拉起整份清单时该服务仍保持 stopped），只有 `start --service`（或对已停服务的 `restart --service`）能解除。要单独回滚一个服务到旧监督器：先 `stop --service` 再按旧监督器流程起它，回切前按旧监督器的不完整 stop 问题核对无孤儿进程。
- `restart --service` 由监督器优雅终止并立即拉起目标服务，**不消耗崩溃退避计数**（`restarts` 不变），其余服务的 PID 与 `restarts` 不动。
- **能力门**：单服务操作要求运行中的监督器是新二进制（state 里 `service_control: true`）。老监督器在跑时会直接报错拒绝且不写任何标记——此时**不要**手工往 state 目录写 `*.stop-request` 文件绕过：老监督器会忽略它，而日后新监督器启动时会突然兑现它。~~2026-10-01 现状：监控栈实例已是新 supervisor；**数据面实例的主进程仍是旧映像**……在那之前数据面单服务操作会被拒绝~~ **2026-10-01 维护窗口后两份清单的 supervisor 均为新映像，单服务操作在数据面也已实测可用**（`restart --service zeek-raw-indexer` 实测：新 PID、`restarts` 不增、身份仍 tuba）。

- `status` 里的 `restarts` 是自本次 `start` 以来的累计重启次数，`backoff` 表示该服务在指数退避中（默认 1s 起、上限 30s）。`backoff` 一定伴随日志里的真实退出原因，先看 `logs` 再动手。
- Launcher 以 root 运行；**自 2026-10-01 起 11 个数据面服务经 `setpriv --reuid=967 --regid=965 --clear-groups --no-new-privs` 以 tuba 账号运行**（混合模型，与监控栈同源；`tuba.env` 等密钥文件保持 0600 root，由 Launcher 读取注入）。`command` 必须是可执行文件本身（setpriv 包装时 `command` 为 `/usr/bin/setpriv`，真实二进制在 `args`），**不能**写成 `python3 <binary>`。
- 服务环境只来自清单显式字段与 0600 环境文件；子进程只额外继承 `PATH/HOME/USER/LANG` 等基础变量。**清单里没有的变量，服务就看不到**，且 Launcher 读环境文件时会 `TrimSpace`，值不能带首尾空白。
- `restart` 会先 `stop`（写 `stop.request`，runner 轮询到后向**整个进程组**发 SIGTERM，10 秒后强杀）再 `start`，因此是一次全量重启；`stop` 用 `LoadManifestForControl`，不读环境文件、不校验二进制，环境文件损坏时仍可停。

**回滚到旧监督器**（保留的退路，切换前形态）：

```
tuba-launcher stop --manifest /etc/tuba/tuba-services.json
python3 /opt/tuba/collector-live/manage_zeek_live_pipeline.py start   # 6 个 zeek 服务
python3 /opt/tuba/collector-live/tenant_a_chain.py start              # 4 个 tenant_a 服务
```

监督器的 `start` 会去 `/proc` 里找**正在运行的 `tuba-api`** 取环境基座，所以顺序必须是"先 api、后监督器"；api 若没起来，监督器会直接报 `source registry has no enabled source contexts` 或取不到环境。旧监督器的 `stop` **不完整**（实测 6 个 zeek 子进程只回收 2 个，其余成为孤儿继续消费），所以回滚或重切之前务必用 `pgrep -af collector-live/pipeline/bin` 核对没有残留进程——两名消费者在同一消费组内会导致索引重复写入。

**开机恢复：cron `@reboot` 受控入口**（2026-09-30 安装，不注册 systemd 仍是设计决定；**2026-10-01 已经两次真实整机重启验收，见「O04 主机重启验收步骤」**）。root crontab 有一行 `@reboot /opt/tuba/bin/tuba-boot`；该脚本（0700 root，**仓库副本 `scripts/tuba-boot.sh`，改动须同步**）向 `/var/log/tuba/boot.log` 写带时间戳的日志后：① 探测并在需要时拉起 TUBA 验证 Kafka broker（10.6.68.248:29292，无 systemd 管理，2026-10-01 重启验收发现其无开机入口后补入）；② 依次对**两份清单**执行 `tuba-launcher start`：数据面 `/etc/tuba/tuba-services.json` 与监控栈 `/etc/tuba/tuba-monitoring.json`。脚本幂等：每份清单先用 `status` 探测，**探测是正证据门槛**——必须匹配到 `^TUBA launcher pid=[0-9]` 头部行才算在运行（2026-10-01 重启验收实测：`status` 对陈旧 state 文件仍退出 0 且打印残留 "running" 行，按退出码判断会误判为已监督；这是 Launcher 代码层的待修缺陷——**已于 2026-10-01 根修并部署**：新二进制（SHA-256 `dc1f3e08…70d0`，sidecar 已更新）的 `status` 对陈旧 state 直接退出非零、打印 `no live supervisor (stale state from boot ...)` 且不再输出陈旧 per-service 行；正证据门槛保留作为幂等双保险，仍须同步维护）。Launcher 的 state 带 `runner_identity`（boot ID + 启动时刻），重启后 PID 被复用也不会被误判为"已在运行"。**外部依赖注意**：共享 PostgreSQL 由另一产品的 adms daemon 管理，2026-10-01 两次重启中它都因自身状态闭锁（STATUS_FILE=1）未拉起 PG，TUBA 数据面因此卡在 PG 连接退避直到人工 `pg_ctl` 拉起——TUBA 的无人值守恢复事实上受这一外部依赖阻塞。**系统日志**：248 的 journald 已于 2026-10-01 改为持久化（`/etc/systemd/journald.conf`：`Storage=persistent`、`SystemMaxUse=512M`，备份 `journald.conf.bak-20261001`）——此前首次重启的关机阶段耗时约 34 分钟因日志只在 volatile `/run` 而无法排查，此后开关机日志跨重启保留，上限 512M。

## 监控栈由 Launcher 纳管（248 现状，2026-10-01 起）

prometheus / grafana / node_exporter / kafka_exporter / capacity-guard 由**第二个 tuba-launcher 实例**监督，清单 `/etc/tuba/tuba-monitoring.json`（0600 root），密钥文件 `/etc/tuba/tuba-monitoring.env`（0600 root，只含 kafka_exporter SCRAM 口令与 Grafana admin 口令两项引用），状态与日志分别在 `/var/lib/tuba/launcher-monitoring` 与 `/var/log/tuba-monitoring`。操作用同一个二进制的另一份 manifest：

```
tuba-launcher status  --manifest /etc/tuba/tuba-monitoring.json
tuba-launcher logs    --manifest /etc/tuba/tuba-monitoring.json --service prometheus --tail 100
```

- **为什么是两份清单而不是并入主清单**：Launcher 的默认 start/stop/restart 是整份清单粒度，且没有运行期 reload；把监控并入 `tuba-services.json` 意味着每次监控变更都要连带重启 11 个数据面服务。两份清单各自有独立 state_dir，互不影响；单服务操作（`--service`，见上节）在其中一份清单内部生效，不跨清单。
- **非 root 身份通过 `setpriv` 保留**：Launcher 以 root 运行且不支持 per-service 用户切换，四个组件的清单 `command` 是 `/usr/bin/setpriv`（`--reuid/--regid/--clear-groups/--no-new-privs` 后 exec 真实二进制），进程身份与切换前完全一致（tuba-prometheus 971、tuba-node-exporter 970、tuba-kafka-exporter 969、tuba-grafana 968）；setpriv 是 exec 语义，PID 不变、SIGTERM 直达服务。capacity-guard 切换前就是 root，保持 root。**不得把 setpriv 包装去掉后直接以前端二进制为 command**——那会让组件以 root 运行，是安全倒退。- **重启单个监控组件**：监控栈实例的 Launcher 已是支持单服务粒度的新二进制（2026-10-01），用 `tuba-launcher restart --manifest /etc/tuba/tuba-monitoring.json --service <名字>`；语义与注意事项（stop 是持久期望状态、跨主进程/整机重启保留、能力门）见上节「单服务操作」。prometheus 的 TSDB 在磁盘上，重启不丢历史。
- 旧的 `scripts/manage_tuba_monitoring.py` 与 capacity-guard 的 `manage_tuba_capacity_guard.py` 保留在磁盘上仅作回滚退路（先 `tuba-launcher stop --manifest /etc/tuba/tuba-monitoring.json` 再用旧脚本 start）；其自带的 log-rotator 已退役，监控组件日志由 Launcher 按 16 MiB×3 轮转（Grafana 自身文件日志仍由 Grafana 内部轮转）。Prometheus 数据目录 `/opt/tuba/monitoring/data/prometheus` 切换前后未动，15 天历史连续。
- **quarantine/standard indexer 的 metrics 端口已于 2026-10-01 修复上线**：此前 248 部署的两个二进制编译于遥测接入之前、无监听（部署滞后），且 zeek 链清单里是继承自旧监督器的错误变量名。已换用 master（49868ef）重建的二进制并把清单变量名改为 `QUARANTINE_INDEXER_METRICS_LISTEN=127.0.0.1:19098` / `STANDARD_INDEXER_METRICS_LISTEN=127.0.0.1:19097`（zeek 链；tenant_a 链仍是 19495/19595）。两端口已监听、`/health/ready` 为 ready；**2026-10-09 第二轮已接线计数器**：worker 接入 metrics registry，`/metrics` 现输出 `tuba_quarantine_indexer_{indexed,invalid,index_failed,retry}_total` 与 `tuba_standard_indexer_{indexed,invalid,index_failed,retry}_total`（零值计数器不渲染，tenant_a quarantine 无新事件时可为空 body，属正常）。四服务（zeek 19097/19098 + tenant_a 19495/19595）经单服务 restart 上线，lag=0、DLQ 零新增。raw-indexer（zeek 19095 / tenant_a 19295）与 normalizer（zeek 19096 / tenant_a 19395）正常监听且有计数。详见 IMPLEMENTATION-TODO「O04 目标机重启验收执行记录」遗留问题第 4 条。

**不要执行 `/opt/tuba/start.sh`**：那是 M1 遗留脚本，会 source `/etc/tuba/tuba.env`。该文件在切换后语义已变——从"api 的完整环境文件"变成"Launcher 的密钥文件"，只含密钥。照旧执行会拉起一个**缺 `ES_URL`** 的 api，症状是日志里的 `ES_URL and ES_API_KEY are required`，而在同一个端口上掩盖掉正常运行的 api。数据面的启停一律经 `tuba-launcher`；`start.sh` 已于 2026-09-30 废止——改名为 `/opt/tuba/start.sh.retired`（0600 root，不可执行，仅留档）。

## O04 主机重启验收步骤（248）

> **已于 2026-10-01 完成两次真实重启验收**（详见 IMPLEMENTATION-TODO 的「O04 目标机重启验收执行记录」）：第一次暴露了 tuba-boot 幂等探测缺陷与 29292 broker 无开机入口（均已修复），第二次全自动恢复成功。以下步骤保留供后续重启窗口复用；注意共享 PG 需人工拉起（见「开机恢复」一节）。

开机恢复入口是 root crontab 的 `@reboot /opt/tuba/bin/tuba-boot`（见上节）：它对数据面 `/etc/tuba/tuba-services.json` 与监控栈 `/etc/tuba/tuba-monitoring.json` 两份清单逐一幂等拉起，日志写 `/var/log/tuba/boot.log`。对照脚本 `scripts/o04_reboot_acceptance.sh` 在运维端（本机 Git Bash）经 SSH 对 248 执行，分 precheck / baseline / verify 三个阶段，**全程只读**：不重启、不停服务、不改配置。SSH 凭据从仓库根 `.env.local` 读取，经 `.codex-ssh-askpass.cmd` 非交互登录。

### 重启前

1. **确认恢复手段在场**。248 没有远程带外管理：若重启后 SSH 不可达或组件未拉起，平台会停在停机状态且无法远程干预，必须有现场登录或其他带外手段兜底才允许执行重启（这也是此前推迟本项的原因）。
2. `bash scripts/o04_reboot_acceptance.sh precheck` —— SSH 连通、crond active、crontab 含 `@reboot` 条目、两份清单全部 running、根盘低于 75%，逐项 PASS 才能继续。
3. `bash scripts/o04_reboot_acceptance.sh baseline --out .runtime/o04-reboot/<标签>` —— 落盘基线：boot_id、`boot.log` 行数水位、两份清单 status（含 restarts）、全部消费组 lag/位点、DLQ topic 末端 offset、ES 各 alias 文档计数、磁盘水位。**基线目录就是验收证据，重启后 verify 直接对账它，不要删。**
4. 核对基线摘要：若采集时已有积压或 failed/backoff 服务，先恢复正常再重启。带积压重启不丢数据（Kafka 12h 保留 + 采集端磁盘队列），但会拉长追平时间。

### 执行重启

由具备 root 与现场/带外条件的运维在 248 上执行（`systemctl reboot` 或带外复位），本脚本不执行重启。记录重启发起时刻。

### 重启后

1. 待 SSH 恢复，执行 `bash scripts/o04_reboot_acceptance.sh verify --baseline .runtime/o04-reboot/<标签>`（`--lag-timeout` 默认 1800 秒）。逐项验收：主机确实重启（boot_id 变化）、tuba-boot 已执行（boot.log 新行）、两份清单全部 running、restarts 计数、活跃消费组 lag 追平耗时（无成员的退役孤儿组不计入）、ES 各 alias 计数 ≥ 基线且 raw alias 恢复增长、DLQ 零新增、消费组集合与基线一致。**全部 PASS（WARN 需逐条确认）才算通过**；verify 幂等可重跑，处置完 FAIL 项后直接再跑。
2. verify 输出连同基线目录归档为 O04/D1 第 3 条证据。

### 未自动恢复时的手动介入

1. **boot.log 无新行**（@reboot 未触发）：查 `systemctl status crond` 与 `crontab -l` 是否仍有该条目；随后手动执行 `/opt/tuba/bin/tuba-boot`（幂等，先 status 探测），再重跑 verify。
2. **tuba-boot 执行了但服务没起来**：读 boot.log 和 `tuba-launcher logs --manifest <清单> --service <名>`。最常见原因是 PG/Kafka/ES 尚未就绪——Launcher 按 1s→30s 退避自动重试，确认依赖恢复后等待即可；`backoff` 状态一定先看日志再动手。
3. **个别服务持续 failed**：修配置/凭据后，两份清单均可直接 `tuba-launcher restart --manifest <清单> --service <名字>`（两实例自 2026-10-01 起均为新 supervisor，能力门已通过）；临时手段也可用 `kill -TERM <pid>` 让 Launcher 按退避拉起（这只管"重启"，没有"保持停止"）。

### 回滚/降级路径

- **Launcher 整体不可用**（二进制或清单损坏）：退回旧 Python 监督器——`tuba-launcher stop --manifest /etc/tuba/tuba-services.json` → 先起 api → `python3 /opt/tuba/collector-live/manage_zeek_live_pipeline.py start` → `python3 /opt/tuba/collector-live/tenant_a_chain.py start`；监控栈退回 `manage_tuba_monitoring.py` 与 `manage_tuba_capacity_guard.py`。**回退前必须 `pgrep -af collector-live/pipeline/bin` 核对无孤儿进程**——两套消费者同组会重复写索引。
- **lag 追平超时但服务全部 running**：不是恢复失败、不回滚，按「TubaKafkaLag」一节逐组排查。
- **ES 计数低于基线**：先排除 capacity guard 到期分区删除（核对其 19100 `/metrics` 的 `deleted_indices_total`），确认是删除还是丢失后再按「恢复 Elasticsearch」处理。

## 滚动部署约束

**共享 `rawevent` 语义的二进制必须同批部署**：`payload_hash` 由 ingest 计算、由 raw-indexer 与 normalizer 校验，adapter 另有一处比对 receipt。只更新其中一部分时，新版生产者产出的信封会被旧版消费者判为 `payload_hash mismatch`，**整条原始流进 DLQ**（2026-09-30 实际发生过约 11 分钟）。当前信封里的 `schema_version` 不区分哈希方案，运行期无法识别混版，只能靠部署纪律避免。

**启用 `file_identity.fingerprint` 会改变事件身份**：位置由 `filebeat-v1:<device>:<inode>:<offset>` 变为 `filebeat-v2:<fingerprint>:<offset>`，`StableID` 随之变化，积压重读会被当作全新事件重复入索引而非去重。要在不产生重复的前提下切换，须在来源暂停时进行。

## 异机备份与恢复

备份由 `scripts/backup_tuba_to_offsite.sh` 每日 02:37 执行。四部分内容分两处落在 `10.6.69.21`：

- **Elasticsearch 快照**直接写进 21 上的仓库 `/opt/tuba-backup/esrepo-248`，该目录 NFS 导出后挂载在 248 的 `/var/lib/elasticsearch/backups`（`path.repo` 路径未变，ES 无需重启）。
- **PostgreSQL 转储（含系统账号摘要和会话）、发布包**经 rsync 推到 `21:/opt/tuba-backup/248/<stamp>/`。免密通道是 248 上一把限定来源地址的密钥。

**RPO 为一次运行间隔。** 没有 WAL 归档：本机 PostgreSQL 与另一产品共用，不得为 TUBA 改动其服务配置。两次运行之间丢失本节点即丢失该窗口的接入数据。

### 为什么仓库必须在别的机器上

旧版本把快照先写进**本机**仓库再推送。这份临时空间（实测 4.9 GB）把 248 根盘从 70% 推到 81%，**越过 ES 的 flood stage 水位（80%）**；ES 随即把**全部索引**置为 `read-only-allow-delete`，raw/quarantine/standard 三个索引器连续 11 分钟被拒写：

```
raw evidence write failed after 5 attempts: raw document write returned 429:
cluster_block_exception: index [...] blocked by: [TOO_MANY_REQUESTS/12/disk usage
exceeded flood-stage watermark, index has read-only-allow-delete block
```

没有丢数据（写失败不提交 offset，组件重启后重读，DLQ 零新增），但流水线每晚会停摆一次且不告警。**这是备份自身造成的**，所以仓库被移出本机根盘，而不是靠事后发现。

脚本在**做任何事之前**先做两项检查，任一不过就**拒绝运行（退出码 5 / 4）而不采备份**：

1. `ES_REPO_PATH` 必须是**独立挂载点**。若挂载缺失（`nofail` 时 21 不可达会出现这种情况），该路径就是根盘上的普通目录，快照会写进本机——正是上面的事故。挂载点之下那个目录已设为 root 只读，即使判断失误 ES 也只会报权限错。
2. 仓库文件系统的预计占用不得超过 `REPO_TARGET_MAX_PCT`（默认 90%）。

```bash
/opt/tuba/backup_tuba_to_offsite.sh --check-only   # 报余量、列现有快照；exit 0 可跑
```

退出码：0 完成（`--check-only` 为可跑）；2 无数据库凭据；3 量不出文件系统或快照大小；4 备份目标余量不足；5 仓库不在独立挂载点上。

每日运行输出落在 `/opt/tuba/logs/backup_tuba_to_offsite.log`。**当前没有"备份未运行"的告警**，拒绝只能靠读这个日志发现——这是一处已知缺口。

### 保留期

仓库是**共享且增量**的：后来的快照复用已有段，所以实测第二个全量快照几乎不增加占用（4.7 GB 不变）。因此**快照不删除**，保留期由脚本经 ES API 执行，只保留最新的 `KEEP`（默认 7）个 `tuba-` 快照；段回收交给 ES。`21:/opt/tuba-backup/248/<stamp>/` 下的转储目录另行按数量保留 `KEEP` 份。

注意 ES 的 REST 列表用 `snapshot` 字段，仓库索引文件用 `name`，读错会静默得到空列表、保留期完全失效。

### 恢复 Elasticsearch

仓库已注册且指向挂载点，正常情况下不需要重建，直接恢复即可：

1. `GET /_snapshot/tuba_offsite/_all` 确认目标快照 `state=SUCCESS`。
2. 用 `rename_pattern` 恢复成临时索引名，核对文档数后再切换别名。

若 21 重建过、仓库内容需要从别处搬回：注销仓库（`DELETE /_snapshot/tuba_offsite`）→ **清空** `/var/lib/elasticsearch/backups` → 拷入备份 → 重新注册（`path.repo` 已声明，无需重启）。"清空"不可省：`index-N` 是仓库世代号，ES 只读最高的那个；把备份叠加到已被改动的目录上，更新的空世代会遮蔽备份中的快照，表现为 `_verify` 通过却列出 0 个快照。

**恢复要求节点有分配余量，这是单节点的硬约束。** `cluster.routing.allocation.disk.watermark.enable_for_single_data_node=true`（默认）会让 **low 水位阻止新分片分配**——不只是副本。本环境该水位现为 **75%**（曾误设为 70%，而节点常态就在 70%，于是恢复**时好时坏**）：磁盘读数越过它时，恢复在几十毫秒内以 `state [FAILURE]` 结束、**没有任何分片启动**，日志里既没有分片级错误也没有异常。诊断要开 `org.elasticsearch.cluster.routing.allocation: TRACE`，日志才会出现：

```
DiskThresholdDecider: node [...] has 72.9% used disk
less than the required 13976562892 free bytes threshold (11.7gb free), preventing allocation
AllocationDeciders: Can not allocate [...]. [DiskThresholdDecider]: NO()
```

`GET /_cluster/allocation/explain?include_yes_decisions=true` 在这种状态下**会误导**：它只给出 `restore_in_progress NO - shard has failed to be restored`，看起来像备份损坏。要做恢复演练，先确认 `df` 明显低于 75%。

### 恢复 PostgreSQL

`pg_restore` 需要目标库；当前 TUBA 数据库身份**没有 CREATEDB 权限**，因此恢复演练必须在独立实例或由具备建库权限的运维身份执行。转储本身已在备份时用 `pg_restore --list` 校验可读（390 个归档条目）。

## 凭据轮转

1. Kafka 服务身份与来源身份均由 `secrets.json` 承载（Kafka 目录下，0600）。轮转后必须重启对应组件，否则旧凭据继续生效直到连接重建。
2. **轮换前先枚举该 SCRAM 用户的全部使用方**：同一身份可能被多个服务、多个命名空间共享。2026-09-30 实测 `zeek-standard-indexer` 与 `tenant-a-standard-indexer` 共用 `tuba-zeek-standard-indexer`，只更新一侧的 env 变量导致另一侧全线 `SASL Authentication failed` 退避；`secrets.json` 与 `/etc/tuba/tuba.env` 中所有持有该口令的字段必须同批更新。变更前备份两份文件，回滚即还原并重启。
3. 来源 API Key 只在创建时返回一次；丢失只能重建来源（会得到新的 source context 与新的 Kafka 身份），因此密钥必须落到受保护存储，不得只依赖终端输出。
4. 来源 Kafka 写权限的撤销通过禁用来源实现；撤销后需确认适配器与 indexer 不再收到该来源数据。
5. 系统登录密码和来源管理员凭据不得写入仓库或聊天记录。本仓库的 `claude.md` 曾把多台主机凭据提交到公开仓库——这类文件必须加入 `.gitignore`，且其中凭据在轮转前一律视为已泄露。

## Topic 删除重建

1. 先导出全部 ACL（`kafka-acls.sh --list`）与目标 topic 的逐条 ACL、分区数与动态配置。KRaft 下 literal ACL **不随 topic 删除而删除**，重建后核对即可，通常无需重加。
2. **topic 删除会连带删除消费组对该 topic 的已提交 offset**。**自 2026-10-01 部署的新版 source-adapter 起，运行期自愈**：停滞 watchdog（60 秒）发现 fetch 无进展后经 broker 元数据探测确认 topic 不存在（仅认 `UnknownTopicOrPartition`，瞬时故障不误判），记录周期停滞日志并抬起 `tuba_source_adapter_stall_events_total` / `topic_missing_events_total` 指标，关闭死 reader；探测到 topic 重建后自动新建 reader，按 `StartOffset: kafka.FirstOffset` 从 0 重读，积压由 receipt 去重吸收，无丢失无重复，**无需重启组件**（2026-10-01 COL-07b 4/5 复验实测：删除后 64 秒检出，重建后 16 秒恢复消费）。历史行为（2026-09-30 及更早版本）：旧版 source-adapter 对 topic 删除+重建**不自愈**，被重建那路静默停滞、无错误日志，必须 `tuba-launcher restart` 才能恢复（2026-09-30 COL-07b 4/5 实测）。若运行的是旧版二进制，仍按此处理。
3. 重建前确认目标 topic 消费组 lag=0，避免删除时丢弃未消费数据。

## 分析面运维（analysis-worker / analysis-sink，2026-10-03 起）

1. **拓扑**：每命名空间一条 analysis 链——`{ns}-analysis-worker`（Python，消费 `tuba.collector.<ns>.attributed.events.v1`，产出 `…analysis.results.v2`）与 `{ns}-analysis-sink`（Go，消费 results.v2，写 ES `ueba-analysis-*` 状态/历史投影）。worker 的处理进度权威是 PG `analysis_checkpoints`（不是 Kafka committed offset 的唯一依据，两者逐消息同步推进）。
2. **积压观测**：`kafka-consumer-groups.sh --describe` 输出**首行是空行**，用 `awk 'NR>1{s+=$5}'` 会把 LOG-END-OFFSET 错当 lag（2026-10-03 实测踩坑，虚报 161k/1.5M 积压）。正确口径：过滤空行与表头后取第 6 列（LAG），并校验数值：`grep -v '^$' | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6}'`。worker 组 lag=0 属正常（贴着 log-end）；sink 组 lag 才反映投影积压。
3. **sink 吞吐**：自 2026-10-03 起 sink 为批量写（`ANALYSIS_BATCH_SIZE` 默认 500 / `ANALYSIS_BATCH_BYTES` 16MiB / `ANALYSIS_BATCH_WAIT` 1s / `ANALYSIS_MAX_ATTEMPTS` 5 / `ANALYSIS_RETRY_BACKOFF` 200ms 指数封顶 6 档，清单环境变量可调）。实测 tenant_a 35–240 msg/s（内容相关）。批末统一提交 offset；DLQ 信封在提交前统一发布，发布失败整批不提交（fail-closed，重启重放幂等吸收）。
4. **RevisionConflict / stale 处置**：`ANALYSIS_REVISION_CONFLICT`（同键同 rev 异内容）进 DLQ，属数据面毒消息，不可恢复、随积压排空出清；`stale revision dropped`（INFO 级）是乱序/迟到修订的正常防护，状态索引保留更高 revision。worker 侧自 2026-10-03 起 revision 链 fail-closed 自愈（`stored_revision+1` 续推），不再出现同 rev 异内容崩溃循环；若 worker 日志出现 RevisionConflict traceback 且 restarts 增长，说明存在比 PG 持久样本更旧的内存状态——先核对是否同组双 worker 并发（RUNBOOK 红线：重复监督），再查 feature_samples 是否被外部清退。
5. **历史追赶**：大批量重放/追赶不要手搓临时二进制；2026-10-03 前 V09 曾用一次性 `cmd/tuba-analysis-catchup`（revision 幂等）追 70k，批量化 sink 上线后正常消费速率已足够，遗留 catchup 代码仅作参考。
6. **诊断**：两 worker 的 metrics 端点 zeek 19112 / tenant_a 19113（`tuba_analysis_processed_events_total`/`emitted_results_total`/`watermark`）。Python 侧现场栈用 `/tmp/py-spy dump --pid <pid>`（若仍在；否则按 2026-10-03 ④ 记录重传）。
7. **隔离修复**：事件级隔离在 quarantine 索引/主题（写保护，修复后按 DLQ 重放流程在隔离租户重放，见「DLQ 重放」）；分析面坏消息隔离在 `tuba.collector.<ns>.dlq.v1`（查询 envelope 的 stage/code/retryable 字段归类，修复源头后重放）。
