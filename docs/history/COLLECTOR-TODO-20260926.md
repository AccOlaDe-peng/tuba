# 已替代的自研 Collector 待办

2026-09-27 起停止按这些旧任务开发；当前 COL 编号已重新定义，历史验收中的编号指旧含义。

Collector 细化任务（首批切片不代表阶段验收；COL-01 是真实来源接入的前置条件）：

- [ ] COL-01（G/D）新增不可变来源上下文与合同；密钥轮换与 epoch 分离；receipt 固定完整可信元数据和 Kafka 确认状态，明确退休上下文与清理边界。
- [ ] COL-02（G）实现 Go Collector、受限确定性过滤引擎和 SQLite WAL 持久队列；先支持 shadow / keep / drop-count，再按事务绑定读取游标、过滤计数和队列入队。
- [ ] COL-03（G）实现单条 HTTP Sender、固定正文重试、回执核对、乱序确认范围、公平调度、退避及本地隔离。
- [ ] COL-04（G/O）完善 Zeek JSON 文件 adapter、来源 dataset 选择、文件身份与代次、rename 轮转、半条记录、截断/缺口检测，并接入跨平台 ZIP 自管理部署形态。
- [ ] COL-05（G/O）实现 Windows Event Log adapter、可审计来源查询、bookmark、频道代次、清空/覆盖检测、原生 XML 保留；沿用统一 ZIP 包与 CLI，不注册 Windows Service。
- [ ] COL-06（G/O）交付配置校验与切换、过滤策略版本/影子命中量/原因码/场景影响、来源心跳、指标、凭证存储、升级恢复和磁盘容量规划。
- [ ] COL-07（G/O）完成强杀、断网、Kafka/PG 故障、磁盘满、轮转、积压期间换 key/版本、异常回执、永久拒绝与端到端容量验收；生产过滤规则须经过代表性时段 shadow、保护场景检查和单来源灰度后逐步执行。
- [ ] COL-08（G）把 Agent enrollment、凭据安全持久化、30 秒心跳、ETag 配置轮询、schema 校验与原子配置切换接入统一 CLI；控制面不可用时继续用本地有效配置和 SQLite 队列，并报告应用版本/失败原因。
- [ ] COL-09（G/O）设计并实现独立 Launcher/Worker 与签名 artifact 发布；包含 OS/架构兼容性、哈希/签名校验、灰度分组、健康确认、断点下载、保留前一版本和失败回滚；不执行平台下发的任意命令。
- [ ] COL-10（G/W）提供租户内 Collector 注册/健康/配置版本/禁用管理界面及审计视图；秘密只显示一次，配置不接收明文 credential。

