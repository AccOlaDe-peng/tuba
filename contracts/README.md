# 机器可读合同

## 当前 Raw 与 UIM v1 合同

- `events/raw/1/schema.json` 定义可信原始信封。租户和来源身份由服务端配置注入；接入方提供来源位置与 JSON 原文。
- `events/standard-event/1/schema.json` 定义 UIM 公共信封和共享领域字段类型；`uim/domain-catalog.v1.yaml` 给出八领域字段要求。`uim/validation-cases.v1.json` 是运行时 Normalizer 的机器用例；当前 parser 覆盖范围仍以 `internal/uim` 的实现为准。
- `events/analysis-result/1/schema.json` 是保留读取的旧结果合同；`events/analysis-result/2/schema.json` 是包含业务对象 revision、generation、input_refs 和固定日期键的结果合同。v1 到 v2 的保守迁移规则在 v2 `contract.md`。
- `releases/1/manifest.schema.json` 定义 TUBA 语义发布包；它与 Beat 二进制 manifest 分离，并列出七类版本化资产、依赖图和运行合同兼容性。
- `events/topics.v1.json` 定义可自动校验的单节点 Topic 目录；`ids.md` 定义稳定 ID 责任与输入。
- Topic 目录中的 `consumers[].consumer_group` 是消费组命名公式，`acl` 的键是 Kafka 身份角色；Source Topic 上 Filebeat/Winlogbeat 映射到逐来源绑定的精确 Topic 凭证。保留期在 A03 容量/RPO 定版前标记为 provisional。
- `api/openapi.yaml` 已包含 API Key 保护的 raw 接入端点。

原认证事件合同作为兼容合同保留，直到全部生产者经过 normalizer。

- `api/openapi.yaml`：Go API 的 OpenAPI 3.1 基线。
- `events/*/1/schema.json`：Kafka 事件主版本 1 的 JSON Schema。
- `examples/`：Go、Python 和 CI 共用的 Golden examples。

同一主版本只允许兼容性增加；破坏性变更必须新建主版本目录和 topic/API 版本。总体语义与兼容要求以 [`../docs/TARGET-ARCHITECTURE.md`](../docs/TARGET-ARCHITECTURE.md) 为准，具体字段以本目录机器可读合同为准。
