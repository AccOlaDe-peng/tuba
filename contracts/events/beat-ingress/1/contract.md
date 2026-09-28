# Beat 接入 Topic v1

状态：实施合同；Topic ACL 与 adapter 尚未部署。机器 schema 见 `schema.json`。

1. 每条 Kafka value 必须是一条 Filebeat/Winlogbeat ECS JSON event object；Kafka message value 不是数组，也不接受批量包装。`agent.type` 只能是 `filebeat` 或 `winlogbeat`，`agent.id` 和 `agent.version` 必须存在。
2. Topic 配置由平台登记为一个不可变 `source_context_id`、organization、namespace、vendor、dataset、release 和 epoch。消费者按收到的 Topic 查服务端绑定；value、Kafka key、Beat 的 `host.*` 或自报 `organization` 都不能改变可信身份。一个 Topic 不得映射多个租户或可变来源。
3. Beat 以独立来源凭据只写其被授权的 Topic。凭据不得拥有 Raw Topic、其他来源 Topic、管理 API 或 Kafka 管理权限。来源上下文停用时撤销写 ACL；Topic 积压按原绑定处理或明确隔离，不得改绑另一租户。
4. Source adapter 以 `topic + partition + offset` 生成不可变传输位置和 `delivery_id`。相同 Kafka 记录重读会得到同一 ID；Beat 因 ACK 不确定而把同一来源记录重新写到新 offset 时，会形成不同传输位置。除非来源适配器另行提供并验证稳定位置，不承诺跨 offset 的来源事件去重。
5. Adapter 保留完整 Kafka value/hash、Beat 版本和来源上下文。厂商字段由 DIP 解析；ECS 不等于 UIM。源消息中的 `organization.*`、`namespace`、`ueba.*`、route、quality、provenance 仅当普通原文保留，不能用于授权或路由。
6. 单条 Kafka value 的接入上限为 **1 MiB**，生产者配置的 `max.request.size`、Broker `message.max.bytes`、consumer `fetch.max.bytes`、adapter 内存上限和 ingest payload 上限必须协调；超限进入有界永久拒绝隔离并可查询，禁止截断成成功。若原文包装使该上限不适用，应通过新合同版本提升所有层限制后再发布。
7. 数据流确认分三段：Beat 仅在 Kafka broker 按配置确认后确认输出；adapter 仅在 TUBA ingest 返回与当前 delivery/source context 匹配的 202 receipt 后提交输入 offset；indexer 仅在 ES 成功/等价重复确认或 DLQ 持久确认后提交。一个阶段的 ACK 不代表后续阶段完成。
8. 临时失败使用相同 Kafka 记录重试，不能重新生成来源位置、上下文或 Raw 正文；永久拒绝写入来源隔离记录后才可提交 offset。adapter 崩溃窗口由重复读取和 ingest receipt 消除，不以异步 `enable.auto.commit` 覆盖手工提交。
9. Filebeat 原始文件行从 `message` 保留，若配置能填充 `event.original`，一并保留；Winlogbeat 原生 XML/事件数据的可用性须由锁定版本实测。没有原文证据的连接器必须声明其证据等级。

## 位置与重建

本合同使用 Kafka offset 作为 adapter 传输去重位置。Topic 新建/重建必须创建新来源上下文或 epoch，并且不能复用旧 consumer group offsets。Beat registry 的 source identity 和磁盘队列按组件版本管理；若 registry 丢失、状态目录重建或插件修改了事件后重发，可能产生新的 Kafka offset。该情况下依赖行为已界定为“可能重复”，需要来源级稳定位置增强才可声称端到端去重。

## 数据样例规则

有效消息至少含 `@timestamp`、`agent.type/version/id`、`event.dataset`。每个 profile 另外要求来源特定数据：Zeek 文件事件需保留完整 `message`、完全相同的 `event.original` 和 `log.file.path`；可信来源 dataset 必须等于 `event.dataset`。DIP 解析 `event.original`，Raw 则保留完整 Beat 包装。Winlogbeat 需保留 `winlog.channel`、record ID 和原生事件证据。缺少 profile 必需字段时写接入 quarantine，记录稳定错误码，不发布可信 Raw；当前 adapter 仅实现通用 Beat 字段校验，profile 准入隔离仍待实现。
