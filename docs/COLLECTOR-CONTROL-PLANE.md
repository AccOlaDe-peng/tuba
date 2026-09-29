# TUBA 采集管理面与组件发布

版本：2.1｜日期：2026-09-29｜状态：设计定版、待实现；替代旧自研 Collector 管理设计

数据流及可靠性合同见 [采集体系设计](COLLECTOR-DESIGN.md)。现有 API 和数据库表可复用，当前 ZIP 程序仍是旧自研采集器；Filebeat/Winlogbeat 管理客户端、来源适配服务和升级闭环尚未实现。历史见 [旧管理设计](history/COLLECTOR-CONTROL-PLANE-20260926.md)。

## 1. 组件职责

TUBA API/PG 管理安装身份、来源登记、不可变来源上下文、Topic ACL、配置版本、组件制品和审计。Management Agent 运行于来源或汇聚主机，管理 Filebeat/Winlogbeat、Syslog 网关及连接器子进程。平台 source-adapter 消费已登记的接入 Topic，经现有 ingest receipt 发布可信 Raw。

管理 API 使用开发 HTTP；Agent 主动出站注册、30 秒心跳和 ETag 配置轮询，不开放远程 shell。Kafka 仅承载数据，单节点维持不变。生产传输保护按 O03 实施。无需 Fleet、Redis 或常驻管理消息通道。

## 2. 身份和配置

复用一次性 enrollment、`col_*` 安装 ID、管理 bearer 摘要与 source:manage/RBAC。collector_id 代表管理代理安装，component_id 标识受管子进程，source_instance_id 标识逻辑来源，source_context_id 固定来源版本与接入 Topic 绑定。

Agent 管理凭据、来源 Kafka 写凭据、adapter 的 ingest 凭据相互独立。禁用来源必须撤销其 Kafka ACL/凭据并阻止新配置，不仅禁用心跳。已在 broker 中的数据按冻结的上下文继续处理或进入明确隔离策略，不能改变积压消息的租户归属。

现有配置 API 的通用 JSON 需升级到明确的版本化 schema：组件类型/版本、来源绑定、允许输入参数、凭据引用、队列限额、过滤策略和期望配置版本。用户不提交任意 Beat YAML、可执行路径、命令或明文密钥；服务端生成受限模板，Agent 二次校验。凭据用受保护本机存储/组件 keystore，正文和日志不得回显。

应用顺序：下载配置→schema 与组件兼容检查→路径/权限/磁盘检查→组件配置校验→写候选版本→受控重启→健康确认→上报 applied_version。失败保持旧版本并记录原因。先建立 Topic、ACL 和 source-context 绑定，再发布采集配置。控制面离线时使用最后有效配置继续采集。

## 3. 进程与包

统一 CLI 提供 enroll/start/stop/restart/status/logs/config-check；不注册 systemd 或 Windows Service。同一管理代理监督多个来源组件，每个来源使用独立 data/registry 和凭据。需实现单实例锁、进程身份校验、崩溃退避、防重复读取、Windows/Linux 优雅停止及孤儿进程恢复。

主机重启自启动是独立交付项；默认手动启动，允许后续明确配置启动入口，不能宣称手动 ZIP 包会自行在重启后恢复。

签名 manifest 固定组件版本、OS/架构、来源地址、哈希、配置与状态格式兼容范围。下载、限速、空间检查、验签、灰度、健康确认及回滚均由管理代理执行。升级 Beat 时验证 registry/disk queue 兼容，保留可恢复检查点；失败不能盲目回滚可执行文件后复用不兼容数据目录。

## 4. 健康和质量

安装在线、组件存活、采集有进展、接入 Kafka 已确认、Raw 已接收、标准索引新鲜度分别展示。心跳继续 30 秒，90 秒离线阈值沿用首片；上报每组件版本、期望/生效配置、队列、最老消息年龄、重启次数、缺口和有限诊断。Beat 指标字段由锁定版本适配，不伪造未提供的逐规则丢弃量。

生产过滤先经过代表性流量 shadow、保护场景检查和单来源灰度。平台过滤量与源端范围排除量分别记录，缺少源端可观测性时明确标记未知。

## 5. 实现状态及顺序

已存在：`00011_collector_management.sql`、`internal/control/collectors.go`、`internal/api/collectors.go` 的 enrollment/heartbeat/config/disable 和租户鉴权；source context 与 ingest receipt。

待实现：管理代理客户端、组件监督器、模板与配置 schema、Kafka 来源凭据/ACL 自动编排与配置下发、source-adapter 生产化及指标适配、组件发布仓库、签名升级与回滚、管理 UI。ingest 已按规范 Topic 名查询 PostgreSQL 来源上下文，adapter 使用内部共享令牌调用此映射路由。开发期已有 `tuba-source-topic-admin` 手动工具：从已启用的 source context 生成计划，显式 `-apply` 时创建并核对单分区 Topic 和精确 ACL；SCRAM 用户创建、撤销/轮换、adapter token 发放/轮换及端到端闭环仍未交付。248 现有 Kafka 通告 `localhost:9192` 且无 ACL authorizer，须先盘点共享 broker 的现有客户端，再安排 listener 和 authorizer 变更。

按 COL-01–COL-15 实施；先合同与受管 Beat 最小链路，再来源覆盖和管理发布能力。普通配置保持现有 API 路径以利复用，字段变化需同步 OpenAPI 与 migrations；本轮文档更新不自动改变运行接口。
