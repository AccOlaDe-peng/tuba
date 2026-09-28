# 备份与灾难恢复

## 目标

| 数据 | 备份 | RPO | RTO |
| --- | --- | --- | --- |
| PostgreSQL 控制面 | 每日全量 + WAL/PITR | 15 分钟 | 2 小时 |
| Elasticsearch | 每日 snapshot + 定期恢复演练 | 24 小时 | 4 小时 |
| Kafka | 多副本 + topic 保留 + 重放能力 | 由 topic 保留决定 | 2 小时 |
| Vault 密钥 | Vault 自身 snapshot | 15 分钟 | 1 小时 |

实际目标需按业务批准值和受管服务 SLA 调整。

## PostgreSQL

本地验证：

```powershell
.\scripts\backup_local.ps1
.\scripts\restore_postgres.ps1 -BackupFile output\backups\postgres-....dump -Database tuba_restore_test
```

生产使用 `backup.postgres.enabled=true` 创建 CronJob，并要求底层 PVC 或对象存储开启加密、版本控制和跨故障域复制。

恢复顺序：

1. 停止 API 和 analysis worker 写入。
2. 在隔离实例恢复最近全量备份。
3. 应用 WAL 到批准恢复点。
4. 校验 organizations、memberships、cases、audit 和 analysis checkpoint。
5. 切换连接并完成权限和案件冒烟测试。

## Elasticsearch

生产必须提前配置共享 snapshot repository，例如 S3、GCS 或受管对象存储。`backup.elasticsearch.enabled=true` 的 CronJob 只负责创建 snapshot，不负责创建存储凭据。

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
- 每半年执行 PostgreSQL 主备切换、Keycloak 不可用和完整区域切换演练。
- 演练报告必须包含实际 RPO、实际 RTO、缺失数据范围、修复项和责任人。
