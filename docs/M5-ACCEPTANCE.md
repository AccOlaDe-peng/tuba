# M5 部署与运营验收记录

验收日期：2026-09-24

## 已交付

- 完整 Helm chart：6 个应用 Deployment、Service、ServiceAccount、PDB、HPA、滚动升级和健康探针。
- Gateway API：HTTPS Gateway 和到 API/Web 的 HTTPRoute。
- Vault Secrets Store CSI：可选 SecretProviderClass 和运行时 Secret 注入。
- NetworkPolicy：Gateway、metrics 和外部依赖端口的最小网络边界基线。
- Prometheus ServiceMonitor、PrometheusRule 和 Grafana dashboard。
- PostgreSQL 和分析 worker 的 Prometheus 指标与 readiness/live 端点。
- PostgreSQL 备份/恢复脚本和 Helm CronJob。
- Elasticsearch snapshot/restore 脚本和 Helm CronJob 模板。
- 负载、故障注入、权限检查和 Helm 升级/回滚脚本。
- 部署、Observability、Runbook 和 Backup/DR 运维文档。

## 本地验收结果

| 检查 | 结果 |
| --- | --- |
| `helm lint` | 通过 |
| 默认 values 渲染 | 通过 |
| 生产 values 示例渲染 | 44 个资源 |
| `kubeconform --strict` | 35 个 Kubernetes 资源有效，0 无效，9 个 CRD 被显式跳过 |
| Go 镜像构建 | 通过 |
| Python 分析镜像构建 | 通过 |
| Web 镜像构建及 `/healthz` | 通过 |
| 镜像运行用户 | 三套镜像均为 `65532:65532` |
| 1000 并发事件、20 workers | 1000/1000 返回 202，1532.03 events/s，P95 14.98 ms |
| 5000 请求、50 workers | 无应用错误；3095 accepted，1905 受控 429，P95 19.07 ms |
| Elasticsearch 故障注入 | 中断时 Lag=50，恢复并重启消费者后 Lag=0 |
| PostgreSQL 备份 | 生成 custom-format dump |
| PostgreSQL 隔离恢复 | 成功恢复；organization=1，case=3 |
| Elasticsearch snapshot | 成功创建 `tuba-*` snapshot |
| 权限检查 | 未认证 401、analyst 管理成员 403、analyst 读异常 200、tenant_admin 管理成员 200 |
| 最终端到端冒烟 | 5 次失败后成功事件生成 1 条高危异常，证据数 6 |
| 回滚脚本 | 已交付 Helm history/rollback 门禁 |
| Go 单元和 `go vet` | 通过 |
| Python 测试 | 8/8 通过 |
| Web 类型检查、测试和生产构建 | 通过 |

## SLO 与恢复目标

当前批准的建议基线：

- API/ingest 月度可用性 99.9%
- API 成功请求 P95 小于 300 ms
- 分析 watermark P95 延迟小于 120 秒
- 跨租户越权成功次数为 0
- PostgreSQL RPO 15 分钟、RTO 2 小时
- Elasticsearch RPO 24 小时、RTO 4 小时

这些值必须在目标生产集群完成容量、长稳、主备切换和灾备演练后由业务和技术负责人正式签字。

## 剩余上线门槛

本地工作机没有目标生产 Kubernetes 集群、HA Kafka、HA PostgreSQL、HA Elasticsearch 和真实 Vault，因此以下项目不能在本地伪造通过：

- 目标集群 `helm upgrade --atomic` 和金丝雀发布
- 生产网络策略对实际中间件 CIDR 的收敛
- 批准吞吐、长稳、P95/P99 和容量水位
- 真实主备切换、Kafka broker 故障和 ES 节点故障
- 真实 RPO/RTO、跨区域恢复和 Vault 轮换演练
- 安全扫描、镜像签名、SBOM 和准入控制器

完成上述目标环境验收后，M5 才能从“工程交付完成”进入“生产发布批准”。
