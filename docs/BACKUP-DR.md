# 备份与灾难恢复

状态：设计定版，环境目标与恢复演练待配置和验收。详细边界见 [产品详细设计基线](DESIGN-BASELINE.md)。

## 目标

| 数据 | 备份 | RPO | RTO |
| --- | --- | --- | --- |
| PostgreSQL 控制面 | 每日全量 + WAL/PITR | 15 分钟 | 2 小时 |
| Elasticsearch | 每日 snapshot + 定期恢复演练 | 24 小时 | 4 小时 |
| Kafka | 单 broker 24h 保留，不作为备份；用 checkpoint、Raw/ES snapshot 重建 | 不单独承诺 | 不单独承诺 |
| Keycloak | 专用 PostgreSQL 每日全量 + WAL/PITR | 15 分钟 | 2 小时 |
| 发布包/配置/审计清单 | 每日复制到异故障域、对象版本不可覆盖 | 24 小时 | 4 小时 |

这些是生产设计目标。当前未指定异机接收端，因此运行状态必须报告 `backup_not_configured`，不能对磁盘或整机损失承诺 RPO/RTO。
备份目标必须是不同故障域的 S3 兼容对象存储或受控备份主机；同一根盘上的目录、卷或容器 snapshot 不构成备份。

## PostgreSQL

本地验证：

```powershell
.\scripts\backup_local.ps1
.\scripts\restore_postgres.ps1 -BackupFile output\backups\postgres-....dump -Database tuba_restore_test
```

生产由 Launcher 管理的备份任务执行每日 base backup 和连续 WAL 归档。目标存储必须开启传输/静态加密和对象版本控制；
凭据通过受限环境文件注入，不写入任务参数、日志或发布包。

恢复顺序：

1. 停止 API 和 analysis worker 写入。
2. 在隔离实例恢复最近全量备份。
3. 应用 WAL 到批准恢复点。
4. 校验 organizations、memberships、cases、audit 和 analysis checkpoint。
5. 切换连接并完成权限和案件冒烟测试。

## Elasticsearch

生产必须提前配置异故障域 snapshot repository。Launcher 管理的备份任务只负责创建、校验和按策略清理 snapshot，不负责生成存储凭据。

恢复演练必须在独立集群执行，禁止直接覆盖生产索引：

```powershell
.\scripts\restore_elasticsearch.ps1 -Snapshot tuba-20260924T144411Z
```

恢复后验证：

- `logs-ueba.authentication-*` 文档数和时间范围
- `ueba-anomalies-*` 文档数、证据引用和分析窗口
- ILM、mapping 和租户 query 过滤
- 从 Kafka 重放恢复点之后的事件

## 演练

- 每日自动备份并记录成功/失败指标。
- 每月在隔离环境恢复 PostgreSQL 最新备份。
- 每季度恢复 Elasticsearch snapshot 并重放一个 Kafka 时间窗口。
- 每半年执行整机丢失恢复、Keycloak 不可用和版本升级/回滚演练；当前单节点版本不宣称主备或区域切换能力。
- 演练报告必须包含实际 RPO、实际 RTO、缺失数据范围、修复项和责任人。
