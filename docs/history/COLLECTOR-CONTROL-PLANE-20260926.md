> 历史设计与验收证据：2026-09-27 已被成熟采集器＋TUBA 管理方案替代，不再作为实施要求。当前设计见 [Collector 设计](../COLLECTOR-DESIGN.md)。

# Collector 远程管理与发布设计

日期：2026-09-26｜状态：服务端管理 API 首批实现；Agent 端到端闭环待实现

本文记录 Collector 在来源主机运行、TUBA 平台集中管理的设计，以及代码现状。主机部署统一为一个可解压 ZIP 目录包，Linux 与 Windows 使用同一 Collector 二进制命令和配置布局；Collector 自己控制启动、停止、重启、状态和日志，不注册 systemd 或 Windows Service。Kafka 由 TUBA 平台使用，Collector 不直连 Kafka。

## 1. 目标拓扑

```text
管理员控制台 / API
        │ OIDC + source:manage
        ▼
TUBA API ───────── PostgreSQL 控制状态
   ▲                    │
   │ HTTPS              ├─ 一次性 enrollment token
   │                    ├─ Collector registry / credential digest
   │                    └─ 不可变配置版本 / 审计
   │ 主动轮询：heartbeat + config +（未来）release metadata
   │
Collector ZIP（来源主机）
  ├─ 本机配置与受 ACL 保护的密钥环境变量
  ├─ adapter → 有界 SQLite WAL spool → HTTPS ingest
  └─（未来）受控 Launcher/Worker 更新与回滚
                              │
                              ▼
                 ingest → Kafka → Raw/DIP/UIM/index
```

Collector 对平台发起出站 HTTPS 连接；平台不需要远程登录来源主机，也不开放来源主机管理端口。业务事件仍经 `/api/v1/ingest/events`，管理心跳/配置使用独立 API。首期单机/少量 Collector 下，PostgreSQL 同时保存注册和配置元数据；配置读取由 Agent 轮询，不引入 Redis、Kafka 管理 topic 或常驻消息通道。

## 2. 生命周期

### 2.1 注册与身份

1. 已授权管理员调用 `POST /api/v1/collectors/enrollments` 创建租户绑定的一次性 enrollment token，默认 30 分钟有效，可设为 5–1440 分钟。明文只在创建响应出现一次，响应禁止缓存；库中只保存 SHA-256 摘要。
2. 操作者在 Collector 包所在主机运行本地 enrollment 命令（客户端尚未实现），Collector 提交 install ID、主机名、OS、架构、版本和 token。
3. 服务端在事务中锁定 token、检查有效期/未消费状态、创建 `col_*` 身份和随机 `tuba_col_*` 凭据、消费 token 并写审计。并发重放只能有一个成功。
4. Collector 凭据只返回一次，客户端应以最小文件权限保存在本机状态目录；服务端只存 credential digest。心跳与配置使用 `Authorization: Bearer tuba_col_*`。凭据泄露时管理员禁用 Collector；后续需要提供显式重新注册/轮换流程。

当前已实现 API 与表：`POST /api/v1/collector/enroll`、`collector_enrollment_tokens`、`collector_agents`。ZIP CLI 尚未提供 enrollment 子命令，不能从当前版本完成真实注册。

### 2.2 心跳与健康

Agent 每 30 秒主动上报一次（当前 enroll 响应给出该间隔），包括版本、当前配置版本、运行状态、spool 深度、最老排队时间和每来源计数/状态。服务端以 90 秒作为 online 显示阈值。状态字段限制为 running、buffering、backpressured、paused、error；sources 限 256 项，只允许固定字段和计数，不接收事件正文或任意 JSON。

当前已实现 `POST /api/v1/collector/heartbeat`，禁用身份在数据库更新谓词中再次校验，避免“认证成功后被禁用、仍更新状态”的竞态。尚无客户端周期心跳和指标采集接线；管理列表因而不会反映实际运行状态。

### 2.3 配置发布与拉取

管理员对租户内 Collector 调用 `PUT /api/v1/collectors/{id}/config` 发布新 JSON 对象。每次发布创建递增版本，不原位覆盖；配置正文上限 64 KiB，发布动作写审计。禁止上传明文 password/secret/token/api_key/credential/private_key 等字段；配置通过 `api_key_env` 引用本机环境变量。不得通过远程配置下发任意 shell 命令或进程启动参数。

Agent 使用 `GET /api/v1/collector/config` 携带 Bearer 凭据和 `If-None-Match` 轮询：有新配置返回版本与 ETag，无变化返回 304，尚无配置返回 204。Agent 必须先本地 schema 校验、检查路径和过滤规则边界、持久化到临时文件并原子切换，随后回报生效版本；校验失败保持上个已知可用配置并回报原因。控制面不可达时，使用已缓存配置和本地 spool 继续运行，不清除数据。

服务端 API 和版本存储已实现。Collector 目前仍仅读本机 JSON 配置，客户端轮询、ETag 缓存、热加载/安全重启、回报 apply 结果尚未接线。首次配置和现有配置的迁移也需要兼容期设计。

### 2.4 二进制发布与回滚（目标能力）

使用独立的 Launcher 管理 Worker 子进程。Launcher 持有稳定本机身份和状态目录；Worker 执行采集。平台发布不可变 artifact metadata：版本、OS/架构、下载 URL、SHA-256、签名、最低兼容配置版本、发布通道和发布时间。Collector 先轮询到期望版本，只在受控通道与兼容条件匹配时下载；验证签名与哈希后解包到新版本目录，保留当前和前一稳定版本。

升级状态：`available → downloading → verified → staged → switching → healthy`；启动/心跳在限定时间内未通过时回到旧版本并报告 `rolled_back`。包签名私钥只在构建/发布环境；来源主机只内置验证公钥。下载需 TLS、限速、临时空间上限和断点恢复；禁止下发任意命令或仅凭 HTTP URL 执行程序。更新期间旧 Worker 先停止读取新日志，等待当前本地事务完成，未确认 spool 保留并由新 Worker继续处理。

本仓库当前没有 artifact repository、签名发布流水线、Launcher/Worker 分离、灰度策略或回滚实现。此段是后续设计要求，不代表远程升级已经可用。首版远程配置稳定后再实施 OTA；建议先单 Collector canary，再按租户/标签分批，失败自动暂停批次。

## 3. 组件职责

| 组件 | 职责 | 运行位置/状态 |
| --- | --- | --- |
| TUBA API | 管理员 OIDC/RBAC；enrollment、列表、配置发布、禁用；Agent enrollment/heartbeat/config | 平台；管理端接口已实现 |
| PostgreSQL | token digest、tenant 绑定、agent credential digest、心跳、配置版本、审计 | 平台；迁移 `00011` 已创建 |
| Collector CLI | 当前管理本机进程，读取来源并通过 HTTPS ingest | 来源主机；不接远程管理 API |
| Agent management client | enrollment、心跳、ETag 轮询、配置校验与应用 | 来源主机；待实现 |
| Launcher/Worker 与 artifact store | 验签、灰度升级、健康检查、失败回滚 | 来源主机/平台；待设计实现 |
| Kafka | 平台内部可靠缓冲和分发事件 | 不供 Collector 直连；首期按单节点决策 |

## 4. Kafka 部署决策

Kafka 是平台内部事件总线，不是远程 Collector 的管理通道。首期单节点安装使用单 KRaft broker、单副本（RF=1）；它能满足开发/验证及可接受 broker 故障时暂时中断的平台。RF=1 不提供 broker 故障数据冗余，Collector SQLite spool 只保护来源到 ingest 的暂存，不保护 Kafka 已确认但平台磁盘损坏的数据。

若生产要求在一个 broker/主机故障后仍持续写入并保留数据，升级至少 3 个独立故障域 broker，关键 topic RF=3、`min.insync.replicas=2`、producer `acks=all`，配置足够副本容量并演练 broker 故障、ISR 收缩、滚动升级和恢复。单机多 broker 不能抵御主机/磁盘整体故障，不应被描述为 HA。最终是否需要集群，依据可接受 RPO/停机时间、日量峰值、保留周期、可用故障域和运维能力决定；不能仅因 Collector 远程管理就引入 Kafka 集群。

## 5. 安全与故障语义

- 管理员操作要求 OIDC 有效租户成员和 `source:manage` 权限；Agent 凭据与用户 token 分离。
- enrollment token 短时、单次使用；凭据仅返回一次、数据库只存摘要；禁用操作审计并立即阻止后续 Agent API。
- 来源 API key 与 Collector 管理凭据用途不同。当前来源 key 存本机环境变量引用；管理配置禁止发送 key 明文。HTTPS/TLS 是跨主机运行要求。
- 心跳只传低基数运维指标、状态与截断诊断，不带日志事件或密钥。指标标签不能使用事件 ID、账号等高基数/敏感内容。
- 平台或网络离线：Collector 使用本机已验证配置继续采集并落 SQLite WAL；按容量阈值背压/暂停来源，恢复后以原位置重试。控制面离线不应触发配置擦除或自动升级。
- 撤销：管理员禁用 Collector，Agent 认证立即失败；来源 ingest key 可独立轮换/撤销，控制面撤销不自动变更来源身份。
- 数据库/磁盘耗尽：拒绝新配置发布或新 Agent 注册并告警；不丢弃本地未确认队列、不自动跳过来源游标。

## 6. 当前代码交付与剩余工作

已落地：迁移 `00011_collector_management.sql`；`internal/control/collectors.go` 实现一次性 token、凭据摘要注册/鉴权、租户列表、心跳、配置版本、禁用和审计；`internal/api/collectors.go` 暴露上述管理/Agent API；OpenAPI 合同已补全。`go build ./cmd/tuba-api ./cmd/tuba-collector ./cmd/tuba-ingest` 与 OpenAPI YAML 解析通过。

尚未闭环：CLI enrollment 命令和安全状态存储；管理 API 客户端、定时心跳、配置 ETag 与原子应用；管理 UI；artifact 存储/签名/灰度升级/回滚；Linux/Windows 真实主机验收；API 权限和并发故障场景验证。迁移需在部署环境独立执行；本次未连接远端服务器、未部署、未执行自动化测试。

关联文档：[Collector 数据面设计](../COLLECTOR-DESIGN.md)、[部署说明](../../deploy/collector/README.md)、[实施 TODO](../IMPLEMENTATION-TODO.md)、[目标架构](../TARGET-ARCHITECTURE.md)。
