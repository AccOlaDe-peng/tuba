# M1 集成验收记录

验收日期：2026-09-24  
环境：Docker Desktop 4.80.0、Docker Engine 29.6.1、Compose 5.1.4、Kafka 4.3.1、Elasticsearch 8.19.22。

## 验收结果

| 场景 | 操作 | 结果 |
| --- | --- | --- |
| 基础设施 | Compose 启动 Kafka、初始化 topic、启动 Elasticsearch | 服务健康；三个 topic 创建成功 |
| ES 资产 | 应用 ILM、component template 和 index template | 资产可由 Elasticsearch API 读取 |
| 正常摄取 | 通过 ingest 提交 100 条 authentication event | 全部返回 HTTP 202；ES 文档数为 100 |
| 指标 | 查询 ingest `/metrics` | `tuba_ingest_accepted_total 100` |
| 坏事件隔离 | 直接向事件 topic 写入坏 JSON | 标准 DLQ 收到 1 条，包含来源、错误码、重试次数和原始载荷 |
| ES 故障积压 | 停止 ES 后继续提交 50 条事件 | ingest 全部接收；消费组显示 lag=50，offset 未提前提交 |
| 恢复追平 | 恢复 ES 并重启 indexer | ES 文档数从 100 增至 150；消费组 lag 从 50 降至 0 |
| 幂等重放 | 重放 10 个已有 event.id | ES 文档数保持 150，409 冲突被视为幂等成功 |

## 结论

M1 的本地集成验收通过：正常链路无丢失，坏事件可定位，Elasticsearch 中断时 Kafka 保留积压，恢复后可以追平，重复投递不会产生重复文档。

本次验收同时修正了 Compose 的 Kafka 内外双监听配置，以及 Compose 5 对 `kafka-init` shell command 的参数解析问题。
