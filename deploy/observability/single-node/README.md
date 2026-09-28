# 248 单节点监控

此目录包含单机 Prometheus scrape 与容量/Kafka 告警。所有 listener 只绑定 `127.0.0.1`；运维从管理机使用 SSH 隧道查看，不直接暴露端口。Prometheus TSDB 使用 15 天/1 GiB 双重上限。主机根盘告警使用 `node_exporter`；A03 的 70/75/80% 策略由 capacity guard 与 Elasticsearch 执行。

248 管理命令：`python3 /opt/tuba/monitoring/manage_tuba_monitoring.py status|start|stop [all|service]`。它不注册 systemd；`start all` 同时启动日志轮转器（单文件 16 MiB，保留 3 份）。

Zeek 数据面整组由 `/opt/tuba/collector-live/manage_zeek_live_pipeline.py start|stop|status` 管理；`start` 与 `start-components` 均通过轻量进程监督器启动组件，启动命令会等待并确认 worker 子进程确实运行。监督器在异常退出后按 2、4、8…秒退避，最大 60 秒；服务稳定运行 5 分钟后退避重置。若只有个别 worker 退出，可用 `start-components <component...>` 仅恢复指定组件，例如 `start-components tuba-source-adapter tuba-normalizer`。该操作要求至少一个现存数据面进程持有原 adapter token；管理器在停止进程前读取 token，并复用现有 Kafka consumer group suffix，不会重置 offset。可用 `python3 /opt/tuba/collector-live/manage_zeek_live_pipeline.py --restart start-components <component...>` 将指定组件安全迁入最新监督器，保持同一消费组并继续现有 offset。`status` 区分 running、restarting 与 stopped/stale；若所有 token 持有进程都停止，恢复命令会拒绝操作。所有控制仍由产品管理脚本执行，不注册 systemd。

访问 Grafana/Prometheus 时，从管理机执行 `ssh -N -L 13000:127.0.0.1:13000 -L 19090:127.0.0.1:19090 root@10.6.68.248`，再浏览 `http://127.0.0.1:13000` 或 `http://127.0.0.1:19090`。Grafana 用户名为 `admin`；随机口令位于 248 root-only 文件 `/opt/tuba/monitoring/secrets.json`。Kafka exporter 使用单独 SCRAM-SHA-512 身份，并且只需 cluster/topic/group 元数据读取权限。不得复用业务、worker 或 Kafka 管理凭据。

外部通知接收器通过 Alertmanager 配置。邮件需要接收地址、SMTP 地址/端口、认证用户名及客户端授权码；QQ 邮箱默认使用 `smtp.qq.com:465`，应使用客户端授权码而非网页登录密码。SMTP 凭据必须保存在 248 root-only 配置中，不写入仓库。没有接收端时，Prometheus 告警表达式仍可本地验收，但不能声称通知闭环完成。
