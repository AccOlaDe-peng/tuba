# TUBA 运维手册

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

1. 确认 Lag 属于 `tuba-raw-indexer-*`、`tuba-standard-indexer-*`、`tuba-quarantine-indexer-*`、`tuba-analysis-*` 还是 `tuba-analysis-sink-*`。
2. 检查对应 Launcher 组件的 CPU、内存、重启次数、readiness 和最近错误。
3. 当前单实例版本不通过临时增加消费者处理故障；若 ES 正常，先确认组件存活、凭据、offset、批量预算和限流状态。
4. 若 ES 出现 `429`、bulk 拒绝或高延迟，先保护 ES，不要扩大读取批量。
5. Lag 下降且 watermark 恢复后关闭事件。

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

## Keycloak/OIDC 故障

1. 已有短期 access token 可能在有效期内继续使用。
2. 停止登录会阻止新会话，但不应绕过 membership 授权。
3. 检查 discovery、JWKS、issuer、audience、时间和证书。
4. 不可用时不要临时关闭 token 验证或放宽 audience。
5. 恢复后执行管理员、分析师、viewer 的权限回归。

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

A03 的单节点边界：根盘 70% warning、75% critical、80% 停止新增写入；Kafka 各 Topic 保留 24 小时，ES Raw/domain/Quarantine 最多 7 个 UTC 日分区。

1. 先分清哪一层在涨。Kafka 与 ES 各有保留期、会自行封顶；**只有 `ingest_receipts` 会无限增长**（每个接入事件一行，约 1.5 KB/行）。2026-09-30 实测：该表 2,511 MB 时为根盘增长主因，写入约 0.5 GB/天。
2. `SELECT pg_size_pretty(pg_total_relation_size('ingest_receipts')), count(*) FROM ingest_receipts;` 确认表状态与最早一行时间。
3. 日常清理由 `scripts/prune_ingest_receipts.sh` 每日 03:17 执行（保留 2 天）。手动核对先不带 `APPLY=true` 试跑，确认待删行数再执行。
4. 保留期不得随意放大：可重投窗口由 Kafka 保留期（24 小时）与采集端 `ignore_older` 界定，2 天已是其两倍余量；而 receipt 若比窗口年轻，放大窗口会一行都清不掉、磁盘继续涨。
5. `DELETE` 只把页面标为可复用，文件不缩小但**增长停止**。要真正把空间还给文件系统须 `VACUUM FULL`，它取 ACCESS EXCLUSIVE 锁、阻塞接入数十秒（适配器保留 offset 重试，不丢数据），只在明确的维护窗口执行。
6. 清理循环若报 `stopped after N batches with rows still eligible`，先确认待删行是否真的归零；该报错曾是计数把 psql 命令标签算作一行所致。

## 采集端(Windows Winlogbeat)运维

1. TUBA 实例与既有采集器**必须隔离**：独立 config、`--path.data`、`--path.logs`，且不注册 Windows 服务。同一主机上可能同时存在别产品的 Winlogbeat，不要改它们的服务或配置。
2. 用 `scripts/manage_windows_winlogbeat.ps1 -Action status|start|stop` 管理；`status` 同时列出“其它实例”PID，用于确认隔离成立。
3. 配置由 `scripts/render_windows_winlogbeat_config.py` 从 `deploy/components` 模板渲染。**渲染产物含来源的 Kafka SCRAM 口令，只写入 gitignore 的 `.runtime/`，不得入库。**
4. 采集器主机上的 `winlogbeat.registry` 记录读取书签。**停止请用脚本的 `stop`；强杀会丢失最多一个刷写周期的书签，下次启动重读那段事件。**
5. 重读本身安全（同一记录按位置去重），前提是 payload 字节稳定：来源字段若随读取变化（如 Windows 渲染的任务名），重读会被判为冲突并进 DLQ。`registry_flush: 1s` 用于缩小这个窗口。
6. 来源未产生数据时，先查 `source_instances` 是否 enabled、Kafka 源 Topic 是否有写入、适配器计数器是否增长，最后才查采集器自身。

## 新增命名空间的数据面

1. 一个命名空间对应一套数据面：raw-indexer、normalizer、quarantine-indexer、standard-indexer 各一；消费组与 metrics 端口都必须与既有链不冲突。
2. `scripts/manage_tenant_pipeline.py` 负责拉起并监督，命名空间、消费组后缀、metrics 端口基址由环境变量覆盖（默认与 tenant_a 部署一致）。
3. ingest 与 source-adapter 是**共用**的：ingest 按来源命名空间把原始事件路由到对应 raw topic，adapter 已消费全部来源 topic，不需要为新命名空间再起一套。
4. 前置条件：该命名空间的 Source Topic、raw/events/quarantine/dlq Topic 与服务身份 ACL 必须已就绪，否则消费组会静默空转。
5. 启动后核对四个组件的 lag，以及对应 ES alias 是否开始出现文档。

## 滚动部署约束

**共享 `rawevent` 语义的二进制必须同批部署**：`payload_hash` 由 ingest 计算、由 raw-indexer 与 normalizer 校验，adapter 另有一处比对 receipt。只更新其中一部分时，新版生产者产出的信封会被旧版消费者判为 `payload_hash mismatch`，**整条原始流进 DLQ**（2026-09-30 实际发生过约 11 分钟）。当前信封里的 `schema_version` 不区分哈希方案，运行期无法识别混版，只能靠部署纪律避免。

**启用 `file_identity.fingerprint` 会改变事件身份**：位置由 `filebeat-v1:<device>:<inode>:<offset>` 变为 `filebeat-v2:<fingerprint>:<offset>`，`StableID` 随之变化，积压重读会被当作全新事件重复入索引而非去重。要在不产生重复的前提下切换，须在来源暂停时进行。

## 凭据轮转

1. Kafka 服务身份与来源身份均由 `secrets.json` 承载（Kafka 目录下，0600）。轮转后必须重启对应组件，否则旧凭据继续生效直到连接重建。
2. 来源 API Key 只在创建时返回一次；丢失只能重建来源（会得到新的 source context 与新的 Kafka 身份），因此密钥必须落到受保护存储，不得只依赖终端输出。
3. 来源 Kafka 写权限的撤销通过禁用来源实现；撤销后需确认适配器与 indexer 不再收到该来源数据。
4. Keycloak 管理员凭据不得写入仓库或聊天记录。本仓库的 `claude.md` 曾把多台主机凭据提交到公开仓库——这类文件必须加入 `.gitignore`，且其中凭据在轮转前一律视为已泄露。
