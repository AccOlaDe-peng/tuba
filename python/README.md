# TUBA Python analysis

Python 只承担窗口分析、特征、统计和模型计算。日志接入、在线 API、权限与 Elasticsearch 最终写入由 Go 服务负责。

```bash
uv sync --frozen
uv run python -m unittest discover -s tests
uv run tuba-analysis-worker
uv run tuba-analysis-replay --input scenarios/auth_failure_then_success.json
uv run tuba-analysis-evaluate scenarios/auth_failure_then_success.json --min-precision 1 --min-recall 1
```

运行 worker 前需设置 `KAFKA_BROKERS`、`KAFKA_EVENTS_TOPIC`、`KAFKA_ANALYSIS_RESULTS_TOPIC`、`TUBA_ORGANIZATION_ID`、`TUBA_NAMESPACE` 和 `DATABASE_URL`。处理状态、水位和运行元数据保存在 PostgreSQL。

详细窗口、checkpoint、回放和反馈规则见 [`docs/ANALYTICS.md`](../docs/ANALYTICS.md)。
