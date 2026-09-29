# 产品文档索引

## 当前实现基线

发生冲突时按以下顺序判断：`TARGET-ARCHITECTURE.md` 定义目标；`IMPLEMENTATION-TODO.md` 定义逐项交付状态；合同、迁移与代码定义当前可执行行为；环境报告和带日期的验收记录只证明记录时的具体状态。历史 M1–M5 文档不可覆盖新的目标决策。

- [TARGET-ARCHITECTURE.md](TARGET-ARCHITECTURE.md)：单节点目标架构、组件边界、数据流、安全、存储、恢复和扩展决策。
- [IMPLEMENTATION-TODO.md](IMPLEMENTATION-TODO.md)：按依赖排序的实现、迁移、运维和验收任务；未勾选项不代表已交付。
- [DESIGN-BASELINE.md](DESIGN-BASELINE.md)：完整单节点范围的详细设计定版；补齐来源、质量、实体、分析、查询、部署、灾备与验收决策，不代表实现完成。
- [COLLECTOR-DESIGN.md](COLLECTOR-DESIGN.md)：采集设计 v2：Filebeat/Winlogbeat、Syslog/API 连接器、TUBA 管理与可信适配、确认边界及旧路径迁移。
- [采集与索引数据流图](diagrams/collector-v4.html)：来源、受管采集、来源 Kafka、可信 Raw、DIP/UIM 与 Elasticsearch 的主流程；已通过 showcase 结构和桌面视口检查。
- [COLLECTOR-CONTROL-PLANE.md](COLLECTOR-CONTROL-PLANE.md)：远程注册、身份凭据、心跳、版本化配置、OTA 目标、Kafka 单节点/集群取舍及当前实现边界。
- [Beat 接入合同](../contracts/events/beat-ingress/1/contract.md) 与 [schema](../contracts/events/beat-ingress/1/schema.json)：Topic 绑定、投递位置、原文保留与三段确认。
- [受管组件版本清单](../deploy/components/manifest.v1.json) 与 [组件打包说明](../deploy/components/README.md)：Filebeat/Winlogbeat 版本、哈希校验和离线包。
- [../README.md](../README.md)：当前仓库能力、开发启动入口和实现状态。
- [../TECH-STACK-BASELINE.md](../TECH-STACK-BASELINE.md)：锁定技术栈和依赖版本。

## 按工作主题查阅

| 文档 | 内容 |
| --- | --- |
| [DEVELOPMENT.md](DEVELOPMENT.md) | 本地初始化、运行和开发命令 |
| [SECURITY-MODEL.md](SECURITY-MODEL.md) | OIDC、租户授权、服务凭据和审计原则 |
| [ANALYTICS.md](ANALYTICS.md) | 现有分析 worker 的窗口、水位、checkpoint、反馈和回放 |
| [FRONTEND.md](FRONTEND.md) | 控制台路由、会话、API 和敏感数据展示约束 |
| [DEPLOYMENT.md](DEPLOYMENT.md) | 已有 Helm 部署制品；当前单节点目标以架构文档为准 |
| [OBSERVABILITY.md](OBSERVABILITY.md) | 指标、SLO、告警和 dashboard |
| [RUNBOOK.md](RUNBOOK.md) | 故障排查和运行操作 |
| [BACKUP-DR.md](BACKUP-DR.md) | 备份范围、恢复顺序和灾备目标 |
| [IMPLEMENTATION-GAP-MATRIX.md](IMPLEMENTATION-GAP-MATRIX.md) | 目标组件与现有代码的映射和缺口 |
| [DATA-MODEL-BASELINE.md](DATA-MODEL-BASELINE.md) | PostgreSQL 表责任、租户边界、迁移顺序与当前数据模型缺口 |
| [ENVIRONMENT-248-REPORT.md](ENVIRONMENT-248-REPORT.md) | 248 指定日期的只读运行环境快照 |
| [CAPACITY-OBSERVATION-20260927.md](CAPACITY-OBSERVATION-20260927.md) | 21/248 容量实测；当前 50 GiB 单节点采集、保留、水位与恢复边界 |
| [PIPELINE-ISOLATION-PLAN.md](PIPELINE-ISOLATION-PLAN.md) | Zeek 隔离验收命名方案；真实隔离验证链路已部署，运行现状见 [环境报告](ENVIRONMENT-248-REPORT.md) |
| [zeek-validation-20260927.yaml](../deploy/profiles/zeek-validation-20260927.yaml) | 已被新方案替代的旧 profile；禁止激活 |

## 历史验收记录

M1–M5 验收文件记录原认证纵切在 2026-09 的本地验收结果，不代表新目标架构已经完成。

- [M1 摄取与索引](M1-ACCEPTANCE.md)
- [M2 身份、租户与案件 API](M2-ACCEPTANCE.md)
- [M3 调查控制台](M3-ACCEPTANCE.md)
- [M4 分析、恢复与回放](M4-ACCEPTANCE.md)
- [M5 部署与运维](M5-ACCEPTANCE.md)

机器可读的 API、事件和存储定义分别维护在 `contracts/`、`migrations/` 和 `elasticsearch/`；Markdown 不复制这些字段定义。

## 采集方案历史

[旧自研采集设计与验收](history/COLLECTOR-DESIGN-20260926.md)、[旧管理设计](history/COLLECTOR-CONTROL-PLANE-20260926.md)、[已替代待办](history/COLLECTOR-TODO-20260926.md) 仅用于迁移复核，不作为新方案实现要求。
