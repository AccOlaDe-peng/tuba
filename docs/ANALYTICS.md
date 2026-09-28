# Python 分析与回放

## 处理边界

Python worker 消费标准化认证事件 topic，按事件时间维护有界实体状态，并将版本化分析结果发布到结果 topic。Go analysis sink 校验租户、合同和结果 ID 后写入 Elasticsearch。

Kafka 消费组是消费位置的第一权威。PostgreSQL 额外保存运行元数据、处理状态和处理 checkpoint，用于重启恢复、迟到数据重算和运营观测。

## 规则 Registry

规则、特征和模型版本位于 `python/tuba_analysis/registry/analysis-registry.json`。每条规则声明：

- 稳定 `id` 和语义化 `version`
- 算法类型和引用的 feature ID
- 阈值、lookback、结果窗口
- 允许迟到时间
- 结果严重度

生产环境可通过 `TUBA_ANALYSIS_REGISTRY` 指向不可变发布的 registry 文件。规则版本变化会改变业务结果 ID，确保新旧结果可以并存审计。

## 事件时间与水位

worker 为每个 Kafka partition 保存独立的处理状态：

- `max_event_time` 是当前已观察到的最大事件时间。
- `watermark = max_event_time - allowed_lateness`。
- 事件时间早于当前 watermark 的输入标记为迟到数据。
- 状态仅保留 `lookback + allowed_lateness` 内的实体事件，避免无界内存增长。

普通事件只发布由当前事件触发的新结果。迟到事件会重算该用户保留窗口内的结果，并使用相同业务结果 ID 覆盖既有异常，同时将 `analysis.reprocessed` 标记为 `true`。

## 持久化与恢复

Migration `00005_analysis_runtime.sql` 和 `00006_analysis_feedback_context.sql` 建立：

| 表 | 用途 |
| --- | --- |
| `analysis_runs` | worker 运行状态、registry 版本、心跳和累计处理量 |
| `analysis_checkpoints` | topic/partition 的下一消费 offset 和事件时间 watermark |
| `analysis_processor_states` | 可恢复的实体窗口状态 |
| `analysis_feedback` | 人工真阳性、误报、漏报和不确定反馈 |

每条消息按以下顺序处理：

1. 计算分析结果。
2. 将结果写入 Kafka 并等待 broker 确认。
3. 保存处理状态和 checkpoint。
4. 提交 Kafka offset。

进程在任一步骤崩溃时，重启后可能重复处理消息，但稳定结果 ID 和 Elasticsearch upsert 不会产生重复异常。若状态写在 Kafka 提交之前，重复输入也会被状态中的事件 ID 去重。

## 回放与评估

```bash
uv run --project python tuba-analysis-replay \
  --input python/scenarios/auth_failure_then_success.json \
  --output replay.json

uv run --project python tuba-analysis-evaluate \
  python/scenarios/auth_failure_then_success.json \
  --min-precision 1 --min-recall 1
```

回放 run ID 由输入事件和 registry 版本计算，因此相同输入会生成字节一致的离线结果。评估输出 TP、FP、FN、precision 和 recall，非零退出表示低于阈值。

## 反馈

案件判定会自动为关联异常写入 `analysis_feedback`。对于没有生成异常的漏报，可通过 `POST /api/v1/analysis/feedback` 提交：

```json
{
  "feedback_type": "false_negative",
  "rule_id": "auth.failure-then-success",
  "entity_id": "missing.user",
  "event_ids": ["event-1", "event-2"],
  "reason": "expected anomaly was absent"
}
```

反馈数据用于后续 Golden Scenario 和离线评估扩展，不直接在线修改生产规则。

## 运行观测

`GET /api/v1/operations/status` 的 `analysis.runtime` 返回最近 run、心跳、处理量、结果量和各 partition checkpoint。控制台“系统运行”页面展示这些信息。
