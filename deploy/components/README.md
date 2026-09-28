# 受管 Beat 组件包

`manifest.v1.json` 将 Filebeat/Winlogbeat 固定为 8.19.0，并记录官方 artifact SHA-512 sidecar 中的校验值。版本与当前 Elasticsearch 8.19 系列同 minor，适用于本地兼容性原型；升级需单独验证配置、registry、队列和 Kafka output 行为。

生成可离线导入的跨平台组件包：

```powershell
.\scripts\package_managed_collectors.ps1 -Tag 8.19.0
```

脚本从 Elastic 官方 HTTPS 地址下载，校验完整 SHA-512 后才解压；也可先将三份上游归档放入 `dist/component-cache`，再用 `-Offline` 生成。归档及 upstream license/notice 文件会保留。产物位于 `dist/managed-collectors/`，该目录默认不提交到 Git。重复 Tag 不覆盖旧产物。

本组件包**不包含** TUBA Management Agent、Kafka 凭据、Topic/ACL 或 source-adapter，不能直接在来源主机启动采集。`zeek-filebeat.example.yml` 是单个 Zeek dataset 的开发配置模板：分别以 `TUBA_ZEEK_DATASET=conn/dns/http/ssl` 运行四个独立 Filebeat 进程，每个进程绑定自己的 `TUBA_SOURCE_CONTEXT_ID`、精确 Topic 写入凭据及 `path.data`/日志目录。活动日志位于 `/opt/zeek/spool/zeek`。上线前需创建四个来源登记、Topic 和 ACL，并提供 Kafka 地址。公开或客户分发仍须完成许可审查；manifest 当前明确没有批准再分发。Management Agent 的模板生成尚未实现。

`source-adapter.example.json` 展示开发期 Topic allowlist 配置结构，不包含来源 API key 或 context ID。adapter 使用环境变量 `SOURCE_ADAPTER_TOKEN` 调用 ingest 内部接入端点；服务端从已登记 Topic 查询并绑定来源身份。正式环境的 token 下发/轮换、Management Agent 配置管理和 Kafka ACL 编排尚未实现。

开发期可用 `tuba-source-topic-admin -context ctx_<32位小写十六进制> -source-principal User:<来源用户> -adapter-principal User:<适配器用户>` 预览登记后的 Topic/ACL 计划；仅显式加 `-apply` 才写入 Kafka。工具要求 `DATABASE_URL`、`KAFKA_BROKERS` 和管理员 Kafka 认证环境变量，先验证 ACL authorizer，再创建并读回 Topic 和 ACL。若中途失败，可能已有部分变更，需检查后重试。工具不创建 SCRAM 用户，也不撤销 ACL。248 当前 Kafka 的 advertised listener 为 `localhost:9192` 且未配置 ACL authorizer，不能直接用于 21 的远程受管 Filebeat；先盘点共享 broker 客户端再变更。

首期 Linux 仅提供 Filebeat amd64；Windows amd64 同包含 Filebeat 与 Winlogbeat。ARM、其他 Linux libc/发行版和 Windows ARM 尚未纳入兼容承诺。
