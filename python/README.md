# TUBA Python analysis

Python 只承担窗口分析、特征、统计和模型计算。日志接入、在线 API、权限与 Elasticsearch 最终写入由 Go 服务负责。

```bash
uv sync --frozen
uv run python -m unittest discover -s tests
uv run tuba-analysis-worker
uv run tuba-analysis-replay --input scenarios/auth_failure_then_success.json
uv run tuba-analysis-evaluate scenarios/auth_failure_then_success.json --min-precision 1 --min-recall 1
```

运行 worker 前需设置 `KAFKA_BROKERS`、`TUBA_ORGANIZATION_ID`、`TUBA_NAMESPACE` 和 `DATABASE_URL`。tuba-analysis-worker 是唯一正式分析调度入口（F08，设计基线 §6）：消费 `KAFKA_ATTRIBUTED_TOPIC`（默认 `tuba.attributed.events.v1`，E04 归因流），经窗口→特征→三检测场景（failure-then-success / failure-burst / baseline-deviation）产出 analysis-result v2 信封到 `KAFKA_ANALYSIS_RESULTS_V2_TOPIC`（默认 `tuba.analysis.results.v2`），并按 registry 中模型条目的 `training.interval_seconds` 周期调度基线训练任务（只读持久化特征样本）。v1 旧路径由 analysis-sink 迁移适配器兼容（F07）；Go 侧 `cmd/tuba-detect-auth` 仅诊断用途，无任何线上调度引用。处理状态、水位、FindingLedger 和运行元数据保存在 PostgreSQL。

详细窗口、checkpoint、回放和反馈规则见 [`docs/ANALYTICS.md`](../docs/ANALYTICS.md)。
