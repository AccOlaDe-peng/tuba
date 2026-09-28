# 新链路隔离命名与切换约定

状态：旧自研链路 profile 已被采集设计 v2 替代，标记 superseded/activationBlocked，尚未激活。组织/namespace 预留可沿用；新版本须补齐每来源上下文接入 Topic、Kafka ACL、Beat 独立 data 目录、source-adapter 消费组和 Raw 确认水位。旧 profile 不可直接用于新方案。
依据：A02 环境快照、N06 隔离验收记录、`contracts/events/topics.v1.json`。

## 隔离验收 profile

| 维度 | 分配值 | 目的 |
| --- | --- | --- |
| `organization.id` | `zeek_validation_20260927_001`（拟建独立测试组织） | 不把验收记录写入 tenant_a 的组织范围 |
| `namespace` | `zeek_validation_20260927_001` | 与 `tenant_a` alias 分开，避免测试写入 `logs-ueba.*-tenant_a` |
| generation | `g1` | 当前 normalizer 和 ES sink 把 generation 固定为 `g1`；在其可配置前，不对同一 namespace 做并行重放或版本切换 |
| 原始 Topic | `tuba.collector.zeek_validation_20260927_001.raw.v1` | 唯一测试输入，不复用既有 validation topic 或默认 `tuba.raw.events.v1` |
| 标准事件 Topic 前缀 | `tuba.collector.zeek_validation_20260927_001.events` | 按 `.<domain>.v1` 形成八个新建领域 Topic |
| Quarantine Topic | `tuba.collector.zeek_validation_20260927_001.quarantine.v1` | 单独验收坏格式/未知类型，不混入现有或正式隔离流 |
| DLQ Topic | `tuba.collector.zeek_validation_20260927_001.dlq.v1` | 本轮失败隔离，不复用已有 validation DLQ 或运行时 `tuba.events.invalid.v1` |
| 消费组 suffix | `zeek_20260927_001` | 通过 `KAFKA_CONSUMER_GROUP_SUFFIX` 为 normalizer、indexer 创建本轮独立组；重复运行要换新 suffix 或显式重置专用组 offset |
| Zeek 来源标签 | `zeek_21` | 管理界面和审计中区分 21 上的来源；实际 source_instance_id 由控制面生成 |

ES 目标 alias 由该 namespace 派生为 `logs-ueba.<domain>-zeek_validation_20260927_001`；物理索引 generation 仍为 `g1`。Raw 与 Quarantine 使用同一隔离 namespace。不得把本 profile 的结果切换到 `tenant_a` alias。

目前来源注册会从 PostgreSQL 组织记录取得 organization/namespace，并要求注册者在该组织有有效身份和发布包。因此实际运行本 profile 前，需要创建独立测试组织、测试身份成员关系和可用 release；当前计划没有创建这些对象，也没有复用已删除的临时登录账号。

## 正式租户路径与旧路径

| 路径 | 处理约定 | 当前证据 |
| --- | --- | --- |
| 正式租户 | `namespace=tenant_a`，使用合同 Topic 和其默认消费组；确认 C05 的 Topic/DLQ 分区、retention 与运行配置一致后启用 | `tenant_a` network alias 已有 2024 与 2026 日期物理索引；raw/topic 已存在，完整八领域 Topic 尚不齐 |
| 隔离验收 | 使用上表唯一 namespace、专用 validation Topic 和唯一 group suffix；保留验收 offset 与文档以供复核 | N06 已有独立 namespace/topic/group 模式；本表分配下一轮 Zeek 命名 |
| 旧认证接口 | `/api/v1/events/authentication` 和专用旧 indexer 路径已删除，不做双写或兼容 | I05 已关闭 |
| Filebeat/其他 Zeek 转发 | 与本次 TUBA 链路无关，不纳入 TUBA 的双写冲突和切换门槛 | 用户确认 21 上 Filebeat 输出到其他链路；不需读取其配置或更改它 |

## 版本化 profile

本次分配已保存为 [`deploy/profiles/zeek-validation-20260927.yaml`](../deploy/profiles/zeek-validation-20260927.yaml)。它是已被替代、禁止直接激活的旧部署输入清单，不会被当前二进制自动加载。profile 明确固定 generation=`g1`；当前写入器只有此代次，隔离性由唯一 namespace、Kafka topic 和消费组提供。不得把它与另一个 generation 并行写入同一 namespace 的生产 alias。

## 启用前检查

1. 先创建并校验 profile 对应的 Topic，确认隔离 Topic 与正式 Topic 的 ACL、分区、RF、retention 符合合同。
2. 确认 `tuba-normalizer`、Raw/标准/Quarantine indexer 使用同一 namespace、topic prefix 和唯一 group suffix；不启动 `tenant_a` 正式消费组。
3. 查验 ES alias 只绑定该验证 namespace 的物理索引；不得复用或移动 `tenant_a` alias。
4. 验收完成后保留数据和 offset；清理由独立的验证数据保留决策执行，不重置/删除生产组。
5. 生产切换前先实现并验证 generation 可配置、别名激活/回滚，再决定从 `g1` 升级到新 generation。当前硬编码 `g1` 使双 generation 并行验证不安全。

因此旧 profile 仅保留命名历史；A04 应先发布 v2 profile，再完成部署验收：创建独立测试组织/来源及所需 release，创建隔离 Kafka Topic，确认 ES alias 落在唯一 namespace，并以独立消费组运行；不涉及既有其他链路 Filebeat 或 `tenant_a` 正式组；新 TUBA Filebeat 必须使用独立状态目录和凭据。
