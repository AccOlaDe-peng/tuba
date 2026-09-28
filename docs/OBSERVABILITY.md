# 指标、SLO 与告警

## 指标入口

所有应用服务提供：

- `/health/live`：进程存活
- `/health/ready`：服务可接收工作；API/ingest 检查其关键依赖，worker 在消费循环运行时报告 ready
- `/metrics`：Prometheus 文本指标

API 与 ingest 在各自业务 HTTP 端口暴露探针和指标。Go worker 默认将探针和指标绑定到 loopback 的独立端口：raw indexer `19095`、normalizer `19096`、standard indexer `19097`、quarantine indexer `19098`、control worker `19099`、analysis sink `19094`；source adapter 默认使用 `127.0.0.1:9185`。可分别用 `<SERVICE>_METRICS_LISTEN` 覆盖；wildcard/非 loopback 绑定必须精确设置 `TUBA_ALLOW_NON_LOOPBACK_LISTEN=true`。Python analysis worker 的 `METRICS_LISTEN` 默认 `127.0.0.1:9090`，也执行同一 opt-in 检查；开发启动脚本绑定 loopback `9093`。Helm 为 API/ingest 和两个 analysis metrics listener 显式启用 Pod 监听，NetworkPolicy 仅允许指定监控命名空间抓取指标。

Go worker 的 ready 表示进程和消费循环仍在运行；Kafka Reader 负责连接重试，ES sink 在有限重试耗尽后让 worker 退出，交由 Launcher 恢复。控制面 worker 额外每 5 秒探测 PostgreSQL 和首个 Kafka broker 的连接及 SASL/TLS 握手，任一依赖不可用时返回 503，恢复后自动返回 200。ready 不代替 lag、失败率或数据新鲜度监控。API ready 探测 PostgreSQL 与 Elasticsearch；ingest ready 探测 PostgreSQL 与首个 Kafka broker。

API/ingest 请求默认由 `HTTP_REQUEST_TIMEOUT=30s` 限时，HTTP server 的读超时跟随请求上限、写超时额外留 5 秒，空闲连接 60 秒关闭。PostgreSQL 连接统一设置 `PG_STATEMENT_TIMEOUT=15s`，范围为 1 秒至 5 分钟；连接池 ping 上限 5 秒。超时会取消 request context 或由 PostgreSQL 中止单条语句；提交状态不明时仍按原有幂等键/receipt 语义重试，不能把超时等同于操作未提交。

回归测试验证 ingest readiness 会随依赖从不可用、恢复到再次不可用切换，也验证缺少 DB/ES 探测对象时 API 返回未就绪；该测试覆盖探针状态逻辑，不替代连接实际 PostgreSQL/Kafka/Elasticsearch 的现场恢复演练。

## 关键指标

| 指标 | 含义 |
| --- | --- |
| `tuba_raw_ingest_accepted_total` | Raw ingest 接受并确认写入 Kafka 的事件 |
| `tuba_raw_ingest_kafka_failures_total` | Raw ingest 写 Kafka 失败 |
| `tuba_source_adapter_events_fetched_total` | Source adapter 从来源 Topic 拉取的事件 |
| `tuba_source_adapter_events_accepted_total` | Source adapter 收到服务端成功回执的事件 |
| `tuba_source_adapter_events_rejected_total` | Source adapter 收到永久拒绝并写入本地 DLQ 的事件 |
| `tuba_source_adapter_delivery_retries_total` | Source adapter 可重试投递次数 |
| `tuba_source_adapter_dlq_write_failures_total` | Source adapter 本地 DLQ 持久化失败 |
| `tuba_source_adapter_offset_commit_failures_total` | Source adapter 提交来源 offset 失败 |
| `tuba_raw_indexer_indexed_total` / `tuba_raw_indexer_index_failed_total` | Raw 索引写入成功 / 失败 |
| `tuba_indexer_events_received_total` | 索引器读取事件 |
| `tuba_indexer_events_indexed_total` | 成功写入 ES |
| `tuba_indexer_events_index_failed_total` | 永久写入失败并进入 DLQ |
| `tuba_indexer_events_validation_failed_total` | 标准索引器校验失败 |
| `tuba_indexer_bulk_retry_total` | Bulk 部分失败重试批次 |
| `tuba_analysis_results_received_total` / `tuba_analysis_results_written_total` | Analysis sink 接收 / 写入结果 |
| `tuba_analysis_results_dead_letter_total` | Analysis sink 坏结果进入 DLQ |
| `tuba_analysis_processed_events_total` | Python worker 已处理事件 |
| `tuba_analysis_emitted_results_total` | Python worker 已发布结果 |
| `tuba_analysis_dead_letter_total` | Python analysis worker 永久拒绝事件数 |
| `tuba_analysis_watermark_timestamp_seconds` | 各 Kafka partition 的事件时间水位 |
| `tuba_api_http_requests_total` | API 请求总量 |
| `tuba_api_http_errors_total` | API 5xx 总量 |
| `kafka_consumergroup_lag` | Kafka Exporter 提供的消费组 Lag；当前应用不自行暴露该指标 |

当前没有 `tuba_ingest_rejected_total`、`tuba_ingest_accepted_total` 等旧名，也没有应用内磁盘剩余空间或 Kafka lag 指标。HTTP 身份/格式拒绝和限流尚未形成 Prometheus 计数，因此不能用它们制作拒绝率告警。单节点监控需另配操作系统磁盘采集器和 Kafka exporter，并确认 Grafana/Prometheus 抓取目标可达；Exporter 未部署前，相应面板无数据是预期状态。

248 的 O05 capacity guard 在 `127.0.0.1:19100/metrics` 提供 `tuba_capacity_disk_used_percent`、`tuba_capacity_level`、`tuba_capacity_delete_candidates` 和 `tuba_capacity_deleted_indices_total`。它同时记录水位状态变化。Prometheus/Grafana 和外部通知状态见下方 248 单节点部署合同与实施 TODO。

### 248 单节点监控部署合同

配置与 dashboard 源文件位于 `deploy/observability/single-node/`，由 `scripts/manage_tuba_monitoring.py` 手动启停，不注册 systemd。所有端口只绑定 `127.0.0.1`，通过 SSH 隧道访问：Prometheus `19090`、node exporter `19101`、Kafka exporter `19102`、capacity guard `19100`，Grafana 预留 `13000`。Prometheus 保留 15 天，TSDB 同时限制为 1 GiB。

Prometheus 抓取 capacity guard、主机文件系统、现场可达的 Raw indexer (`19095`) 和 Kafka exporter。其余 worker 端口只有在对应进程运行且该版本启用 metrics listener 后才应加入目标清单。Kafka exporter 使用 Kafka KRaft broker 独立 SCRAM-SHA-512 principal `tuba-kafka-observer`；ACL 仅授予 cluster Describe、`tuba.` 前缀 Topic Describe，以及 `tuba-` 前缀 consumer group Read/Describe。口令只在 248 Kafka root-only `secrets.json` 中保存，并经进程环境传递，不放入命令行参数。Exporter 和告警规则进一步按当前 Zeek profile 的消费组后缀过滤，避免旧验证组残留造成误报。

首期告警覆盖根盘 70/75/80% 水位、capacity guard/node exporter/Kafka exporter 不可用、TUBA consumer lag > 10,000 持续 10 分钟，以及“存在积压但 consumer group 没有活动成员”持续 5 分钟。Prometheus 告警可本地查看；外发通知需要 Alertmanager receiver 和环境方提供的 webhook、邮件或企业微信接收配置。接收配置未提供前不声称通知闭环完成。Grafana dashboard 已以文件 provisioning 形式提供。

## SLO 基线

| 能力 | SLI | 目标 |
| --- | --- | --- |
| 认证接入 | 可用性和成功接收率 | 月度 99.9%，无静默丢失 |
| API | HTTP 可用性 | 月度 99.9% |
| API 延迟 | 成功请求 P95 | 小于 300 ms |
| 分析新鲜度 | 当前时间减 watermark | P95 小于 120 秒 |
| DLQ | 未处理永久失败持续时长 | 15 分钟内进入处置 |
| 租户隔离 | 跨租户越权成功次数 | 0 |
| 控制面恢复 | PostgreSQL RPO / RTO | RPO 15 分钟，RTO 2 小时 |
| 检索恢复 | Elasticsearch RPO / RTO | RPO 24 小时，RTO 4 小时 |

目标值应在上线前用批准的生产容量模型复核。

## 告警

Helm 的 `PrometheusRule` 默认包含：

- `TubaIngestKafkaFailures`：Kafka 写入失败速率大于 0，持续 10 分钟
- `TubaSourceDeliveryRetries`：来源投递持续重试，持续 10 分钟
- `TubaSourceDLQWriteFailures`：来源永久拒绝事件无法落本地 DLQ，持续 5 分钟
- `TubaIndexerFailures`：索引永久失败持续 10 分钟
- `TubaAnalysisStale`：watermark 落后超过 5 分钟
- `TubaAnalysisDeadLetter`：analysis worker 或 sink 在 15 分钟内产生永久拒绝/死信
- `TubaAPIErrors`：5xx 比例超过 2%
- `TubaKafkaLag`：Tuba consumer group Lag 超过 10000，持续 15 分钟

告警抑制、路由和值班升级由集群 Alertmanager 配置管理，建议 critical 5 分钟触达、warning 30 分钟汇总。

## Dashboard

`observability.dashboard.enabled=true` 创建 Grafana sidecar ConfigMap，包含：

- Raw ingest accepted/Kafka failures
- Source adapter accepted/retried/DLQ failures
- indexer indexed/failed
- analysis watermark age
- Kafka consumer lag（依赖 Kafka Exporter）
- API 5xx ratio

日志应使用结构化字段，并至少包含 `service`、`environment`、`request_id`、`topic`、`partition`、`offset` 和错误码。禁止记录 token、API key、密码和原始敏感载荷。
