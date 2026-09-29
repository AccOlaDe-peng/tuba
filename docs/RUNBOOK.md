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
