# Python 分析与回放

## 处理边界

Python worker 消费标准化领域事件 topic，按事件时间维护有界实体状态，并将版本化 feature、baseline、anomaly 和 risk contribution 结果发布到结果 topic。Go analysis sink 校验租户、合同、generation、revision 和结果 ID 后写入 Elasticsearch。

PostgreSQL 的 inbox、业务状态、checkpoint 和 outbox 是业务恢复权威；Kafka committed offset 是运输水位，必须与 PG checkpoint 对账。两者不一致时从 PG checkpoint 安全重读，不自动跳到 Kafka latest。完整事务协议见 [产品详细设计基线](DESIGN-BASELINE.md)。

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

普通事件只发布由当前事件触发的新结果。迟到但仍在保留边界内的事件重算同一窗口，使用相同业务结果 ID 和递增 revision 发布修订；失效结果发布 `retracted`，不原地覆盖历史。超出边界的数据只能通过受控回填进入新 generation。

## 持久化与恢复

Migration `00005_analysis_runtime.sql` 和 `00006_analysis_feedback_context.sql` 建立：

| 表 | 用途 |
| --- | --- |
| `analysis_runs` | worker 运行状态、registry 版本、心跳和累计处理量 |
| `analysis_checkpoints` | topic/partition 的下一消费 offset 和事件时间 watermark |
| `analysis_processor_states` | 可恢复的实体窗口状态 |
| `analysis_feedback` | 人工真阳性、误报、漏报和不确定反馈 |

每条消息或有界批次按以下协议处理：

1. 在一个 PostgreSQL 事务内写 inbox 去重、处理状态、checkpoint 和 outbox。
2. outbox publisher 按聚合键顺序发布结果并等待 Kafka broker 确认。
3. 标记 outbox 已发送，再提交不早于 PG checkpoint 的 Kafka offset。
4. 崩溃重发依赖稳定 message/result ID、revision 和 sink 幂等处理。

进程在任一步骤崩溃时可能重复读取或发布，但不会跳过未提交业务状态。Elasticsearch 按业务 ID 和 revision 拒绝旧值覆盖；同 revision 不同内容进入冲突隔离。

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
