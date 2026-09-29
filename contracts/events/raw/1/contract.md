# Raw envelope v1 接入规则

本合同表示一条来源记录。每个 `POST /api/v1/ingest/events` 正文只能是一个 JSON object；不接受 JSON array、批量包装对象或多行 JSON。来源文件中的每条完整记录单独提交，并各自获得 source position、`raw_event_id` 和 receipt。批量 HTTP 接口若后续增加，必须逐项返回 receipt，且不能改变本合同的单记录语义。

## 可信字段

`organization.id`、`namespace`、`source_instance_id`、`source_context_id`、`source_epoch`、`vendor` 和 `release_id` 由服务端来源注册表绑定，客户端不能通过事件正文设置或覆盖。`source_position` 是稳定的来源记录位置，最大 512 字节；同一来源代次内必须唯一且重试时不变。Filebeat 文件事件由 adapter/ingest 从 Filebeat 生成的 `log.file.device_id`、`log.file.inode` 与行起始 `log.offset` 组成 `filebeat-v1:<device>:<inode>:<offset>`；缺任一坐标时退回 Kafka delivery position，并将该数据集标记为无法保证跨 Kafka offset 去重。其它采集器应使用各自稳定的源端游标。

`delivery_position` 是可选的传输审计位置。Beat Kafka 接入使用 `kafka-v1:<topic>:<partition>:<offset>`；它不是 Raw 事件身份的一部分。相同源文件行在新的 Kafka offset 重投时，必须复用 Raw receipt/ID；receipt 响应仍回显本次 `delivery_position`，adapter 校验后才提交本次 offset。位置复用但 payload 不同会得到冲突，不能以 payload hash 替代位置去重。

`source_context_id` 格式为 `ctx_` 加 32 位小写十六进制。组织和 namespace 只允许 ASCII 字母、数字、下划线和连字符，最多 64 字符。来源实例、来源代次与 release 最多 128 字符；vendor name/product/dataset 各为 1–128 字符。

## 大小、时间与 ID

`payload` 必须是 1 MiB 以内的 JSON object。HTTP 接入在构造完整 Raw envelope 前限制 payload 大小；schema 不通过字符串字符数近似计算 JSON 字节数。超限事件拒绝为 413，由 Collector 保留未确认游标并上报。

`received_at` 是接入端生成的 UTC 接收时间，客户端不能指定。来源发生时间保留在 payload 中，由 DIP 根据来源语义解析；它不改变 Raw envelope ID 或按来源位置去重的规则。

`raw_event_id` 固定为 `raw:` 加 64 位小写 SHA-256 hex，哈希输入为版本前缀以及 organization、source instance、dataset、source epoch、稳定 source position 的长度编码序列。密钥轮换不改变 epoch；日志清理/回退等来源游标重建才需要通过控制面推进 epoch。Filebeat 上游至少一次重投因而映射为同一个 Raw ES `_id`，而不是重复的 Raw/domain 事件。

## 错误隔离

格式错误、缺失/越权可信上下文、source position 重用冲突、大小超限必须返回明确的非成功 HTTP 状态，不能以 202 接受。服务端只有在数据库 receipt 持久化且 Kafka 写入确认后才返回 202。客户端必须保留未确认记录及其原始 position，重试时正文和 position 保持不变。
