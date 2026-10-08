# TUBA

TUBA 是面向 Windows Security 与 Zeek 等来源的 UEBA 平台。本仓库正在从早期认证事件纵切迁移到完整单节点架构；目标和当前完成度以 [目标架构](docs/TARGET-ARCHITECTURE.md) 与 [实施 TODO](docs/IMPLEMENTATION-TODO.md) 为准。TODO 未勾选项仍未通过该项验收，代码切片和历史 M1–M5 记录不表示完整产品已交付。

## 文档入口

- [文档索引与权威顺序](docs/README.md)
- [目标架构](docs/TARGET-ARCHITECTURE.md)
- [实施 TODO 与当前代码切片](docs/IMPLEMENTATION-TODO.md)
- [产品详细设计基线](docs/DESIGN-BASELINE.md)
- [运行环境 248 盘点](docs/ENVIRONMENT-248-REPORT.md)
- [Collector 设计](docs/COLLECTOR-DESIGN.md) 与 [Collector 控制面](docs/COLLECTOR-CONTROL-PLANE.md)
- [本地开发](docs/DEVELOPMENT.md)

## 已确认的采集方案

采用 Filebeat/Winlogbeat、Syslog 网关和专用连接器，由 TUBA Management Agent 统一管理；经来源接入 Kafka 和新增 source-adapter 接入现有 ingest/Raw/DIP/UIM。此为待实现目标，当前自研采集代码冻结新增通用采集需求，部署尚未替换。详见 [采集设计 v2](docs/COLLECTOR-DESIGN.md)。

## 当前实现切片

- Go 数据面已有 Raw 接入、来源凭证解析、Kafka receipt、Raw 索引、首批 Zeek/Windows Security DIP/UIM 映射、标准事件索引与隔离处理。
- Windows Security 已**在 139/169 上真实接入并闭环**：Winlogbeat 独立实例 → 来源 Kafka（SCRAM 身份与精确 ACL）→ source-adapter → tuba-ingest → Raw ES → DIP/UIM → 标准 ES。该链路按来源命名空间路由，与既有 Zeek 链路并行、不共用消费组。逐段数量见 TODO 的 2026-09-29/30 记录。
- 语义发布（release）控制面已实现并部署：manifest 注册、validate→stage→activate 状态机、资产哈希与依赖校验、全局 publisher 授权与审计；`windows-security-1.0.0` 已 active。
- Collector 仍有 Zeek JSONL、SQLite WAL 队列、单条 HTTP 投递和过滤骨架；该自研路径保留迁移用途，Windows 自研 adapter 已取消。
- 控制面已有 Collector 注册、心跳、凭据与配置版本 API；**Agent 客户端尚未接通**。因此 Windows 采集器目前由运维脚本手工部署与管理，未纳入远程管理闭环。
- **尚未交付**：TUBA Management Agent 与其远程配置下发（COL-08/09）、来源过滤策略版本与影子计数（COL-06）、任务/outbox、实体、特征、风险与管理查询控制台。
- Web 登录验收已通过开发 realm 的用户名/密码登录；生产身份接入和目标运行环境的登录验收不由此推定完成。

组件与数据流的目标、已有落点和缺口逐项记录在 [实施差异矩阵](docs/IMPLEMENTATION-GAP-MATRIX.md)。248 当前运行状态以 [环境盘点报告](docs/ENVIRONMENT-248-REPORT.md) 为准；该报告是指定日期的快照，不是持续健康保证。

## 本地开发

依赖启动和开发命令见 [DEVELOPMENT.md](docs/DEVELOPMENT.md)。快速入口：

```powershell
.\scripts\bootstrap_local.ps1
.\scripts\start_local.ps1
```

本地开发身份配置与远程开发 realm 不同。只在本地使用的示例用户及凭据以开发文档和本机 `.env` 为准，不要复用到共享或生产环境。

## 运行架构

目标数据流为：

```text
来源 → Beat/网关/连接器 → Kafka 接入 → source-adapter → tuba-ingest → Kafka Raw
                              ├→ Raw Indexer → Elasticsearch 原始证据
                              └→ Normalizer（DIP + UIM）→ 标准事件 Kafka
                                                       ├→ 标准 Indexer → Elasticsearch
                                                       └→ Entity / Feature / Detection / Risk workers
```

首期目标是各平台功能单实例运行。TUBA Management Agent 和 Filebeat/Winlogbeat 采用跨 Linux/Windows 的统一目录包与 CLI，不注册 systemd 或 Windows Service；平台服务统一由产品 Launcher/CLI 控制。Kafka、PostgreSQL、Elasticsearch 与身份服务的当前开发部署细节和未完成运维工作见目标架构及 TODO。

系统登录已集成到 TUBA：同源用户名/密码登录、服务器会话及数据库 RBAC；247 Keycloak 不属于系统登录依赖。配置和账号迁移见 [系统登录](docs/SYSTEM-LOGIN.md)。
