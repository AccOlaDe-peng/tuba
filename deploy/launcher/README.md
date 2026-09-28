# TUBA Launcher 单节点安装

本目录提供跨 Windows/Linux 的同一 JSON 服务清单和 `tuba-launcher` CLI。Launcher 负责启动、停止、重启、状态、日志尾读和进程退出后的指数退避重启；它不注册 systemd 或 Windows Service。Linux 向子进程组发送 SIGTERM；Windows 子进程运行在隐藏的独立控制台进程组，Launcher 通过 Ctrl-Break 通知 Go 服务优雅退出，10 秒仍未退出时才强制终止。Kafka、PostgreSQL、Elasticsearch 和 Keycloak 的安装/初始化属于 O02。

## 构建发布包

在仓库根目录运行：

```powershell
.\scripts\package_tuba.ps1 -Version 0.1.0 -GOOS linux -GOARCH amd64
.\scripts\package_tuba.ps1 -Version 0.1.0 -GOOS windows -GOARCH amd64
```

每个包包括同一组 TUBA Go 可执行程序（含 `tuba-topic-admin`、`tuba-source-topic-admin`、`tuba-kafka-security-admin` 初始化工具）、对应平台的服务清单模板、Source Adapter 配置示例、Kafka Topic 合同和 SHA-256 sidecar。打包前 `scripts/verify_tuba_package_stage.ps1` 强制校验归档文件 allowlist、敏感服务配置必须使用 `${ENV_NAME}` 精确引用、示例凭据必须是占位符；local-dev realm 与实际 `.env` 不在复制清单中。该校验防止误把运行配置打进发布包，但不能替代运行时日志审查与秘密轮转。

`tuba-topic-admin --namespace <slug>` 默认只输出 Topic 计划；只有显式指定 `--apply` 才会创建/核对 Topic。当前仅允许合同中的 `zeek_validation_single_node` 有界验证 profile：单 partition、单 replica、`min.insync.replicas=1`、24 小时 retention。已存在但配置不符合 profile 的 Topic 会报错，不会自动改写。该命令不创建 source-context Topic 或 ACL。

`tuba-kafka-security-admin --namespace <slug>` 默认只输出服务 principal 和 literal ACL 计划；只有显式指定 `--apply` 才会创建/更新 validation profile 声明的 SCRAM-SHA-512 凭据并添加、读回核验服务 ACL。Apply 需要 Kafka 管理员凭据通过环境变量注入，并需要已受保护环境文件中的各角色密码（`TUBA_INGEST_KAFKA_PASSWORD` 等）。该工具只添加合同中当前 profile 声明的服务 ACL，不撤销既有 ACL；source-context Topic/来源凭据仍由 `tuba-source-topic-admin` 在校验数据库登记后分别处理。所有命令只支持有界 validation profile；生产保留值待 A03 定案。

安全初始化先创建 Topic，再按相同 namespace 执行 `tuba-kafka-security-admin --namespace <slug> --group-suffix <suffix> --apply`。未指定 group suffix 时生成合同中的基础 consumer group ACL；如果消费者使用独立后缀，必须传入完全一致的 suffix。CLI 不打印任何密码，服务用户名必须与所选 profile 对应，密码从受保护环境文件注入。CLI 当前以 additive reconcile 管理 ACL：它不会撤销既有或过期 ACL；任何身份退休需单独审查并按服务状态执行。

## Linux

以 root 安装：

```bash
sudo bash scripts/install_tuba_linux.sh 0.1.0 dist/tuba-0.1.0-linux-amd64.tar.gz
```

安装目录为 `/opt/tuba/releases/<version>`，`/opt/tuba/current` 指向当前版本；配置位于 `/etc/tuba`，运行状态位于 `/var/lib/tuba`，日志位于 `/var/log/tuba`。安装器创建不可登录的 `tuba` 系统账号，程序目录由 root 拥有，状态/日志由 `tuba` 独占。Launcher 以该账号手动运行，不安装服务管理器条目。

先编辑 `/etc/tuba/tuba-services.json`、`/etc/tuba/source-adapter.json` 和 `/etc/tuba/tuba.env`，再用 `runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher start --manifest /etc/tuba/tuba-services.json` 启动。环境文件是简单的 `NAME=value` 格式，不执行 shell 展开；Linux 必须为 `0600`。安装器只在目标文件不存在时创建示例，升级保留配置。

## Windows

管理员 PowerShell：

```powershell
.\scripts\package_tuba.ps1 -Version 0.1.0 -GOOS windows -GOARCH amd64
.\scripts\install_tuba_windows.ps1 -Version 0.1.0 `
  -ArchivePath .\dist\tuba-0.1.0-windows-amd64.zip `
  -RunAs .\TubaSvc -CreateLocalAccount
```

`-CreateLocalAccount` 会交互提示该专用本地账号的密码。安装器在 `C:\Program Files\TUBA\releases\<version>` 保存只读版本，`current` 是指向当前版本的 junction；`C:\ProgramData\TUBA` 保存配置、状态和日志。环境文件 DACL 仅允许指定账号、SYSTEM 和本机 Administrators 访问；Launcher 启动时拒绝 Everyone、Authenticated Users 或 Users 的允许 ACE。切换到该账号运行 `tuba-launcher.exe start|stop|restart|status`。

## 服务清单和密钥

清单声明命令、工作目录、重启退避区间及传给每个服务的环境字段。敏感环境值必须写成 `${ENV_NAME}` 引用。实际值从受限环境文件读取；Launcher 不把解析后的秘密写入参数、状态文件或 Launcher 日志，服务只得到清单显式声明的变量和基础 OS 环境白名单。示例清单为 API/ingest 设置 30 秒 `HTTP_REQUEST_TIMEOUT`，数据库服务设置 15 秒 `PG_STATEMENT_TIMEOUT`；后者可在 1 秒至 5 分钟内调整。默认退避从 1 秒增长到 30 秒；可在单个 service 中用 `restart_min` 和 `restart_max` 覆盖。

Launcher 状态文件仅含进程 PID、重启次数和最近退出状态。服务日志保存在独立文件；每个服务日志约束为 16 MiB，超过限制时轮转，最多保留 3 个旧文件（每服务约 64 MiB，上限可能多出一个写入块）。Launcher supervisor 日志在下次启动前检查并轮转达到 16 MiB 的文件，最多保留 3 份。`logs --tail N` 读取当前活动日志，最多读取末尾 4 MiB/1000 行；轮转文件可在日志目录中按 `.1` 至 `.3` 查看。该上限只约束 TUBA Launcher 管理的进程日志，不代替操作系统、Prometheus/Grafana 或 Kafka/ES 的磁盘与消息保留告警；监控采集和磁盘阈值仍由 O05/A03 收口。

## 验收边界

当前已覆盖 Windows/Linux 双平台构建、Launcher manifest/密钥校验、Windows 本机 start/status/stop 回环，以及 Linux/Windows 包构建和哈希检查。Linux 包 sidecar 在 WSL Alpine 用 `sha256sum -c` 验过，归档可完整列出；Alpine v3.23 rootfs 安装器与升级切换通过。另在一次性 Ubuntu 24.04 容器执行最新 Linux 安装包，`tuba` 用户及权限核查、非 root Launcher start/status/stop 均通过，容器以 init 作为 PID 1 且无宿主数据挂载。Alpine nologin 路径差异已由安装器自动发现处理。Windows Launcher 的 Ctrl-Break 已通过隐藏控制台子进程回环验收：子进程捕获 `os.Interrupt`、运行清理并正常退出，连续运行 3 次通过。Ubuntu 裸机目标安装和 Windows 专用账号安装后的 ACL/生命周期仍未验收；当前 Windows shell 非管理员，不能完成提权安装。账户 ACL、升级切换和服务依赖故障演练通过后才可关闭 O01。容量门槛 A03 未定，安装包只用于受控开发/验收，不构成 248 生产部署批准。
