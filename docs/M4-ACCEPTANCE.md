# M4 分析生产化验收记录

验收日期：2026-09-24

## 交付范围

- 版本化 feature、rule、model registry。
- 基于 Kafka partition 的事件时间状态、watermark 和允许迟到时间。
- PostgreSQL 持久化的 `analysis_runs`、`analysis_checkpoints` 和 `analysis_processor_states`。
- 迟到数据重算，以及稳定业务结果 ID 和 Elasticsearch upsert。
- 非法 JSON、跨租户输入和永久处理错误的 analysis DLQ。
- Go analysis sink 的重试、DLQ、提交顺序和幂等覆盖测试。
- 确定性离线回放 CLI、Golden Scenario 和 precision/recall 评估 CLI。
- 案件判定自动反馈及误报、漏报、真阳性和不确定反馈 API。
- 系统运行页面展示 worker heartbeat、处理量、结果量和 checkpoint 水位。

## 验收结果

| 场景 | 结果 |
| --- | --- |
| Go 单元与接口测试 | `go test ./...` 通过 |
| Go 静态检查 | `go vet ./...` 通过 |
| Python 单元测试 | 7/7 通过 |
| Golden Scenario | precision 1.0，recall 1.0 |
| 同窗口重复回放 | 两次输出 SHA-256 完全一致 |
| 跨租户输入 | 单元和接口测试拒绝并写入 DLQ |
| 状态恢复 | 4 条失败事件处理后重启 worker，继续处理第 5 条失败和成功事件 |
| 恢复后检测 | 生成异常 `anom:c43a...ec85c`，包含 6 条证据 |
| Kafka 提交恢复 | analysis sink 在映射错误期间保持 offset，映射修复后自动追平 |
| 运行元数据 | PostgreSQL 记录 run 状态、heartbeat、处理量、结果量和 partition checkpoint |
| 人工反馈 | `POST /api/v1/analysis/feedback` 成功写入误报反馈 |

## 结论

M4 完成条件满足：

- 重放同一窗口得到确定性、相同业务结果。
- worker 崩溃后可从 PostgreSQL 恢复事件时间状态，不会丢失后续检测。
- 跨租户输入被拒绝，不能进入结果写入路径。

当前实现采用 at-least-once 消费和确定性结果 ID 加 upsert，不宣称 Kafka 端到端 exactly-once。Kafka 消费组 Lag、生产指标和告警仍按计划在 M5 补齐。
