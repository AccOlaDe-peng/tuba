# 部署基线

- `docker/`：Go（`go.Dockerfile`）、Python worker 和 React 静态站点的可重复镜像构建。
- `helm/tuba/`：完整生产 chart，包含安全上下文、网络策略、HPA/PDB、Gateway、Vault/Secret 注入、监控告警、备份和生产 values 示例。

本地依赖由 `product/compose.yaml` 提供。正式环境不得直接使用 Compose 中的开发密码、单节点 Kafka 或单节点 Elasticsearch。

### Windows 单节点包安装与回滚（O01）

`scripts/package_tuba.ps1 -Version <版本> -GOOS windows -GOARCH amd64` 生成带 SHA-256 sidecar 的 ZIP 包。以提升权限的 PowerShell 运行安装器；`-CreateLocalAccount` 会交互式创建专用本地标准账号，不提供密码参数：

```powershell
.\scripts\install_tuba_windows.ps1 `
  -Version 1.0.0 `
  -ArchivePath .\dist\tuba-1.0.0-windows-amd64.zip `
  -RunAs tuba `
  -CreateLocalAccount
```

安装器校验包哈希和 ZIP 路径，创建版本目录及受 ACL 保护的配置、状态、日志目录；首次安装复制示例 manifest/environment。升级时先完成新版本 ACL 与配置校验，再要求旧 Launcher 正常停止，最后切换 `Program Files\TUBA\current` junction。升级前的 junction 保留为 `Program Files\TUBA\previous`，旧版本文件也保留。安装器遇到切换失败会恢复旧 junction 并尝试重新启动旧版本。Launcher 仍由专用账号运行，不注册 Windows Service。主机启动后由 TUBA Management Agent 或运维启动入口以该账号调用 Launcher；当前阶段未交付 Management Agent，因此目标机重启验收采用运维显式调用，不把 Launcher 描述为操作系统自动启动项。

升级后确认服务健康。如需回滚，在提升权限的 PowerShell 中执行：

```powershell
.\scripts\rollback_tuba_windows.ps1
```

回滚脚本要求 `current` 和 `previous` 都是目录 junction，先通过当前 Launcher 停止服务，再交换两个版本链接并启动恢复版本；启动失败时保留链接状态和日志供排障，不删除任何版本目录或业务数据。当前真实验收覆盖隔离 Windows junction 的首次激活、升级、回滚链接交换及无效 release 失败关闭；专用账号的实际安装与完整 TUBA 服务部署仍待目标 Windows 主机验收。

### Linux 单节点包安装与回滚（O01）

Linux 安装器采用相同的版本目录与 `current`/`previous` 语义。它会验证既有 `tuba` 账号不是 UID 0，也不属于 `root`、`sudo` 或 `wheel` 组，并要求 `nologin` shell；升级前用现有 Launcher 校验配置，再停止进程并切换链接。首次安装/升级命令：

```bash
sudo bash scripts/install_tuba_linux.sh 1.0.0 dist/tuba-1.0.0-linux-amd64.tar.gz
```

回滚使用 root 运行 `sudo bash scripts/rollback_tuba_linux.sh`。它确认两个链接都解析到 `/opt/tuba/releases/` 下的有效版本，正常停止 Launcher、交换 current/previous，再启动恢复版本；版本文件和业务数据不删除。双版本安装、升级与回滚可在隔离 Ubuntu 容器中验收：

```powershell
python scripts/verify_tuba_linux_install.py `
  --first-version 1.0.0-rc9 `
  --first-package dist/stage2-rc9/linux/tuba-1.0.0-rc9-linux-amd64.tar.gz `
  --second-version 1.0.0-rc10 `
  --second-package dist/stage2-rc10/linux/tuba-1.0.0-rc10-linux-amd64.tar.gz
```

该验收容器使用 `--network none`、只读工作区挂载、独立文件系统和 init；覆盖 SHA-256、专用非 root 账号、目录权限、安装升级、Launcher CLI 与独立回滚，不连接产品依赖或 248。升级 manifest 候选在 `/var/lib/tuba` 的私有目录中由 `tuba` 生成和验证，再由 root 提交到 `/etc/tuba`；`tuba` 账号不需要、也不应获得配置目录写权限。它不等于 Ubuntu 裸机安装验收。

### 服务监听边界（O03，开发 HTTP）

`tuba-ingest` 与 `tuba-api` 默认只绑定 `127.0.0.1`；`HTTP_LISTEN`/`API_LISTEN` 配置为 wildcard 或非 loopback 地址时，必须显式设置 `TUBA_ALLOW_NON_LOOPBACK_LISTEN=true`，否则启动失败。Helm chart 仅为需要经 Kubernetes Service 访问的 API/ingest 显式启用此选项；单机部署保持默认 loopback，并由受控反向代理转发外部流量。Vite 开发服务器按路径将 `/api/v1/ingest` 转发到 ingest，将其他 `/api` 转发到 API；可运行 `python scripts/verify_dev_http_proxy.py` 验收 loopback 转发。开发 Compose 的 Kafka、PostgreSQL、Elasticsearch 和 Keycloak host ports 均绑定 `127.0.0.1`，Kafka 容器间通信使用独立 Docker 网络 listener。开发按要求使用 HTTP；生产 TLS、安装版代理和真实目标机防火墙仍需单独验收。

Compose 默认的 Keycloak 只用于本机开发：从 `deploy/keycloak/local-dev/` 导入演示 realm，H2 数据目录使用独立 named volume。部署 realm 使用 `deploy/keycloak/tuba-realm.json`，不包含演示用户，关闭 Direct Access Grants，要求 PKCE S256；部署前需要设置实际回调地址和受保护的数据库/管理员凭据。

### Keycloak PostgreSQL profile

开发单节点可在现有 PostgreSQL 实例中为 Keycloak 建独立数据库和登录角色，不与 TUBA 应用库共用 schema。`deploy/keycloak/compose.postgres.yaml` 是 `compose.yaml` 的 overlay；它用 `keycloak-db-init` 创建/校准数据库角色与数据库，Keycloak 使用 `KC_DB=postgres`。数据库角色不具有 SUPERUSER、CREATEDB、CREATEROLE、REPLICATION 或 BYPASSRLS 权限。初始化容器连接 PostgreSQL 管理账号；在受管环境应使用临时、受限的数据库引导身份，完成后撤销该身份，不把它交给 Keycloak。

先复制 `.env.example` 到未纳入版本控制的 `.env`，设置独立、强随机的 Keycloak 数据库密码及管理员密码。PostgreSQL profile 对 `KEYCLOAK_DB_ADMIN_USER/PASSWORD`、`KEYCLOAK_DB_PASSWORD`、`KEYCLOAK_ADMIN` 和 `KEYCLOAK_ADMIN_PASSWORD` fail closed，不提供弱默认值；管理员数据库账号只供一次性角色/数据库引导，部署到受管 PostgreSQL 时使用短期、受限的引导身份。示例中的 `*-local-only` 值仅供隔离开发，不能用于共享/生产环境。

保护包含这些值的 `.env`：Linux 将部署目录限制为 `0700`、环境文件设为 `0600` 并只允许部署账号读取；Windows 移除文件继承 ACL，仅向部署账号和受控运维管理员授予读取权限。不要把 `.env` 复制到安装包、Launcher manifest、容器镜像或提交到版本控制。Compose 会把 DB/Admin 密码传给容器环境，因此有权访问 Docker daemon 的管理员仍能检查容器配置；应相应限制 daemon 和主机管理权限。启动命令：

```bash
docker compose -f compose.yaml -f deploy/keycloak/compose.postgres.yaml \
  --profile identity-postgres up -d keycloak
```

服务端口默认只绑定 `127.0.0.1:8181`；H2 开发 profile 仍使用 `127.0.0.1:8180`。该 profile 首次启动时从 `deploy/keycloak/tuba-realm.json` 创建 realm。`--import-realm` 只用于新 realm 的首次导入；重启时不会把 JSON 当作 realm 升级工具。

realm 变更按显式升级处理：先备份 Keycloak 专用 PostgreSQL 数据库，再用 Admin REST 导出当前 realm/配置作为受保护回滚证据。`scripts/update_keycloak_web_client.py` 是首个版本化升级操作：更新既有 `tuba-web` redirect URI/origin 前，必须提供非空数据库备份；脚本导出并以 Unix `0700/0600` 或 Windows 当前账号 ACL 保存变更前 realm，拒绝 Direct Access Grants 已开启的客户端，完成更新后读回核对目标字段。运行示例：

```bash
# KEYCLOAK_ADMIN_USERNAME/PASSWORD are loaded from the protected operator environment.
KEYCLOAK_URL=http://127.0.0.1:8181 python3 scripts/update_keycloak_web_client.py \
  --redirect-uri 'https://console.example/*' \
  --database-backup /secure/backups/keycloak-before-realm-change.dump \
  --snapshot-dir /secure/backups/realm-snapshots
```

开发环境按要求可用 HTTP redirect；部署环境使用真实控制台 origin。不要在运行中覆盖 realm JSON 后重启来升级既有 realm。涉及数据库 schema 的 Keycloak 版本升级另按对应版本迁移说明执行，不能与 realm 配置升级混为一谈。

可以用独立 Compose project 演练完整流程，避免碰到默认 `product` 项目正在使用的 PostgreSQL、端口或卷：

```bash
docker compose -p tuba-keycloak-pg-validation \
  -f compose.yaml \
  -f deploy/keycloak/compose.postgres.yaml \
  -f deploy/keycloak/compose.postgres.validation.yaml \
  --profile identity-postgres up -d keycloak
```

验证 realm discovery 后重启 Keycloak 并再次查询；重复运行 `keycloak-db-init` 应成功且保留 realm。确认不再需要临时数据后，使用同一组 `-p`/`-f` 参数执行 `down -v`；该命令会删除这个验证项目的数据库卷。不要对 `product` 项目执行 `down -v`。

### Disposable full-stack initialization profile

`deploy/validation/compose.one-node.yaml` supplies loopback ports for validating a fresh single-node dependency stack without sharing the `product` Compose volumes. Combine it with the base, runtime, and Keycloak PostgreSQL overlays under a unique project name:

```bash
docker compose -p tuba-one-node-validation \
  -f compose.yaml \
  -f deploy/validation/compose.runtime.yaml \
  -f deploy/keycloak/compose.postgres.yaml \
  -f deploy/validation/compose.one-node.yaml \
  --profile identity-postgres up -d kafka postgres elasticsearch keycloak
```

The profile binds PostgreSQL/Kafka/Elasticsearch/Keycloak to `15432/19094/19200/18181` on loopback. Apply migrations and runtime-role provisioning, reconcile validation Topics, apply ES templates, and verify Keycloak discovery with the same project. Use a unique project name; only remove that validation project and its named volumes after the checks. This disposable stack proves initialization order and repeatability, not production capacity, retention, or target-host installation.

### O04 依赖恢复验收

`deploy/validation/compose.runtime.yaml` 将隔离验收 PostgreSQL/Kafka 绑定到 `15434/19094`。启动真实 ingest 后，依赖未启动时应看到 live=200、ready=503；先启动 PostgreSQL，ready 仍为 503；再启动 Kafka，ready 应在不重启 ingest 的情况下恢复 200。若要验证运行中故障恢复，先运行 `python deploy/validation/dependency_fault_proxy.py`，让 ingest 连接代理端口 15435/19095，再通过仅监听 loopback 的控制端口 19096 调用 `POST /targets/postgres/blocked|open` 和 `POST /targets/kafka/blocked|open`；每个依赖都应产生 ready `200→503→200`。完成后向 ingest 进程发送正常中断信号，并确认服务退出和监听端口释放，再用同一个验证项目执行 `down -v` 清理。该流程只写入带唯一 project 前缀的临时卷，不连接 `product` 项目数据库。

验证 Launcher 的真实命令行和监督器生命周期，可运行 `python scripts/verify_launcher_cli.py`。脚本在系统临时目录构建 Launcher 与信号处理 helper，通过 CLI 检查 manifest、启动/状态/日志、重启、优雅停止和 helper 清理标记；不连接任何依赖服务或 248。它验证通用 Launcher 控制路径，不代替业务服务在目标主机上的完整部署及重启恢复验收。

Linux 可用 `bash scripts/verify_launcher_boot_recovery.sh /opt/tuba/current/bin/tuba-launcher` 验证持久 state 中的旧 PID 被无关进程复用时不会阻止恢复。该脚本会强制结束它自己创建的临时 supervisor/helper，只能在隔离验收主机运行，不能指向正在承载业务的 manifest。它验证 PID 与进程身份的组合判断，不等于真实主机重启。

目标机重启验收必须记录重启前后的 boot ID、Launcher state、服务 PID、ready 状态和依赖恢复时间。因为产品明确不注册 systemd 或 Windows Service，重启后由受控运维入口或后续 Management Agent 以专用账号执行：

```bash
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher start --manifest /etc/tuba/tuba-services.json
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher status --manifest /etc/tuba/tuba-services.json
```

验收通过条件是：旧 boot ID/进程身份不能被当作当前 supervisor；所有配置完整的服务重新进入 running，live/ready 按依赖状态恢复；旧状态不会误杀复用同一 PID 的无关进程；日志和 state 仍只允许专用账号及受控管理员读取。依赖未准备好时 ready 保持 503 是正确行为，不能通过重启循环掩盖依赖故障。

## PostgreSQL 初始化与运行账号（单节点实施中）

`DATABASE_URL` 是 TUBA 服务使用的低权限账号；`DATABASE_MIGRATION_URL` 只供初始化/升级脚本使用。对**全新、空白且专供 TUBA 使用的数据库**，先以迁移管理员按文件名顺序应用 `migrations/*.sql`，再创建/校准运行账号：

```bash
DATABASE_MIGRATION_URL='postgres://tuba:REDACTED@127.0.0.1:5432/tuba?sslmode=verify-full' \
  ./scripts/apply_postgres_migrations.sh

DATABASE_MIGRATION_URL='postgres://tuba:REDACTED@127.0.0.1:5432/tuba?sslmode=verify-full' \
TUBA_MIGRATION_DB_USER=tuba \
TUBA_RUNTIME_DB_USER=tuba_runtime \
TUBA_RUNTIME_DB_PASSWORD='REDACTED' \
  ./scripts/provision_postgres_runtime_role.sh
```

迁移脚本只接受 `DATABASE_MIGRATION_URL`，不会回退使用服务的 `DATABASE_URL`。它在 `public.tuba_schema_migrations` 保存每个迁移的完整文件 SHA-256，并用 PostgreSQL advisory lock 串行化多个初始化进程；同版本同哈希会跳过，已应用文件发生变化会失败关闭。每个迁移在独立事务中执行。运行账号脚本撤销 runtime 角色的超级用户、建库、建角色、复制、绕过 RLS 和继承角色权限；只授予业务表 DML、序列使用，以及迁移角色未来新建业务对象的默认 DML。`public.tuba_schema_migrations` 不授予 runtime 读取权。脚本还会撤销数据库对 `PUBLIC` 的 CONNECT 和 `public` schema 对 `PUBLIC` 的 CREATE，因此**不得用于共享数据库**；迁移管理员凭据应只存放在受保护的运维配置中，不传给服务 Launcher。

`scripts/verify_postgres_migrations.ps1 -Container <一次性PG容器名>` 必须显式指定隔离容器，避免误连产品数据库。已在一次性 PostgreSQL 18.6 环境验证迁移首次/重复、双进程并发、checksum drift 失败关闭、失败 DDL 回滚、runtime 角色幂等校准、新表 DML 成功、DDL 及迁移账本访问拒绝。该证据不代表已批准对 248 现存 schema 执行迁移，也不替代生产升级流程；已有库需要先单独核对并建立显式迁移基线。PostgreSQL 14.23 迁移 preflight 仍单独记录于 [实施 TODO](../docs/IMPLEMENTATION-TODO.md)。

Elasticsearch 当前的 canonical template 由 `scripts/generate_es_templates.py` 从事件 Schema 生成，并提交在 `elasticsearch/generated-v1/`。本机 ES 8.19.22 已通过 API 对 24 个模板/映射资产的重复 PUT 与读回核验；本机 Kafka 4.3.1 的 15 个 validation Topic 已创建、核验并清理。另在一次性隔离 Kafka 4.3.1 KRaft broker 上验收了 authorizer、SCRAM、逐服务 ACL、重复 ACL reconcile、无授权写入拒绝及重启后 default-deny；临时资源已清理，未接触 248。新增 `tuba-kafka-security-admin` 已在一次性 broker 实际 apply/重复 reconcile 六个 SCRAM 服务身份与 61 个 literal ACL，并完成读回核验。realm PostgreSQL 持久化和完整单节点依赖栈已通过一次性隔离初始化/重复执行验收；Ubuntu 裸机、Windows 专用账号与 248 目标部署仍未验收。Keycloak Compose 的 local-dev 首次导入、H2 卷权限与密码 token 流程已通过独立项目验收。以上剩余项以 [实施 TODO](../docs/IMPLEMENTATION-TODO.md) 的 O02 为准。

### Elasticsearch 服务账号（O03 进行中）

Linux/Windows Launcher 清单分别引用五个 ES API key 环境变量：API、Raw Indexer、Quarantine Indexer、Standard Indexer、Analysis Sink。用 `scripts/manage_elasticsearch_api_keys.py plan --namespace <namespace>` 生成权限预览；该 CLI 按 namespace 创建五个互不复用的 API key。角色只授权对应 namespace 的 Raw/UIM/Quarantine 物理索引，以及当前 API 必需的逻辑 alias、anomaly 和 case 索引，不授予模板、ILM 或安全管理权限。权限定义见脚本的 `role_descriptors()`；旧的全局 query/indexer 角色文件已移除，避免把宽泛角色误用于数据面服务。

| 服务 | 授权范围 |
| --- | --- |
| API | `monitor`；当前 namespace 的日志 alias/物理索引、anomaly 与 case 索引读取；case fallback 对自身 case 索引允许 `index`。 |
| Raw Indexer | 当前 namespace Raw 物理索引和 Raw alias：建索引、创建文档、读取重复文档、管理该 alias。 |
| Quarantine Indexer | 当前 namespace Quarantine 物理索引和 Quarantine alias：建索引、创建文档、管理该 alias；无读取权。 |
| Standard Indexer | 当前 namespace UIM 物理索引和领域 alias：建索引、创建文档、读取重复文档、管理该 alias。 |
| Analysis Sink | 仅当前 namespace anomaly 索引：建索引和 `index`（对应 sink 的稳定 ID 覆写写入）；无读取权。 |

Elasticsearch 没有 alias-only 内置 privilege；管理脚本改用已在 ES 8.19.22 验收的 action patterns `indices:admin/create`、`indices:admin/aliases*`、`indices:data/write/bulk*`、`indices:data/write/index:op_type/create`，并只作用于该服务的数据集与 namespace。Raw/Standard 的 mapping 修改和物理索引删除均实测拒绝；不要扩大这些 patterns 或授权到 `tuba-v1-*`。

创建一代新 key 时，从受保护环境中提供 `ES_URL` 与 `ES_ADMIN_API_KEY`，再执行：

```bash
python3 scripts/manage_elasticsearch_api_keys.py create \
  --namespace tenant_a --expiration 90d \
  --output /secure/tuba-es-keyring-20260928.json
```

输出 keyring 含有一次性可读的 API key 编码值；文件在 Unix 上限制为当前账号 `0600`，Windows 上移除继承 ACL 并只授予当前账号。Bootstrap 管理 key 不写入 keyring。把五个 `encoded` 值分别放入 `tuba.env` 对应的 `TUBA_*_ES_API_KEY` 变量，并限制环境文件只允许 Launcher 服务账号读取。

轮换采用重叠窗口：先 `create` 新一代 key，把服务环境切换到新 key 并逐个确认服务 ready/写入，再对旧 keyring 执行 `python3 scripts/manage_elasticsearch_api_keys.py revoke --keyring <old-keyring.json>`。撤销后安全销毁旧 keyring。`revoke` 只使用 keyring 中的 key IDs，不打印或传输旧 secret。ES 管理凭据只用于受控初始化/轮换流程，不能进入 Launcher 清单或数据面服务环境。

开发 Compose 的 ES 仍关闭 XPack 安全。实际授权验收使用隔离 `deploy/validation/compose.elasticsearch-security.yaml`（HTTP、本机回环端口 19200、独立卷、ES 8.19.22）；`scripts/verify_elasticsearch_api_keys.py` 验证五种服务的允许读写、跨 namespace/跨职责拒绝，以及旧 key 撤销后新 key 持续有效。运行时授权是否验收通过以 TODO 中记录的当次结果为准；静态权限预览不构成运行验收。开发阶段继续使用 HTTP；生产 TLS、反向代理和监听面仍属于 O03 后续门槛。

验证 Raw、Quarantine、Standard Indexer 和 Analysis Sink 使用各自 ES key 的纵向链路，可运行 `python scripts/verify_indexer_identities_runtime.py`。该脚本构建四个真实索引服务，创建唯一临时 Compose 项目，在回环端口启动 Kafka 4.3.1 与启用 XPack 的 ES 8.19.22，为五类服务生成短期 namespace key，并向四条输入 Topic 投递合同有效记录；检查各服务写入预期索引、写入后 Kafka offset 提交、analysis-sink key 跨 namespace 写入被拒，最后确认四个进程可优雅停止。正常结束时脚本主动撤销 key 并执行 `compose down --volumes`；端口冲突或验收失败时检查脚本输出中的清理告警。该验收不连接现有 `product` Compose 或 248，也不替代 API 查询验收或目标机安装验收。

验证 API 专用 ES key 与用户鉴权闭环，可运行 `python scripts/verify_api_es_identity.py`。该脚本启用同一隔离 Compose 的 `api-identity` profile，在 PostgreSQL 应用 Goose Up migration、用本地开发 Keycloak realm 签发 OIDC token，并为测试 analyst 建立临时 membership；随后启动真实 API，确认有效用户能查询租户事件、无 membership 用户收到 403，API key 不能读取其他 namespace alias，最后通过中断信号停止 API。测试结束后主动撤销 key 并清理临时 Compose volumes。该验证使用仓库的 local-dev realm，不用于生产身份配置。

验证 normalizer、control-worker、source-adapter 的就绪与优雅停止，可运行 `python scripts/verify_remaining_worker_shutdown.py`。脚本只启动隔离项目中的 Kafka/PostgreSQL，应用 Goose Up migrations，为 source adapter 创建一个空的验证 Topic；确认三个服务各自 `/health/ready` 后发送平台正常中断信号，并检查进程退出码。它会回收临时容器、卷和测试目录，不连接现有 `product` 项目或 248。

Topic 初始化 CLI `tuba-topic-admin` 从 `contracts/events/topics.v1.json` 生成物理 Topic 计划，默认仅预览。当前唯一可应用的 profile 是 `zeek_validation_single_node`：

```bash
/opt/tuba/current/bin/tuba-topic-admin --namespace zeek_validation_20260927_001
KAFKA_BROKERS=127.0.0.1:9092 \
KAFKA_SECURITY_PROTOCOL=plaintext \
  /opt/tuba/current/bin/tuba-topic-admin --namespace zeek_validation_20260927_001 --apply
```

`--apply` 创建缺失 Topic，并核验 partition、replica、`min.insync.replicas`、retention 与消息上限；不匹配时失败而不修改现有值。Kafka 的批量建 Topic 请求不是事务，若部分创建失败，修复原因后重跑即可核对并补齐。`tuba-kafka-security-admin --namespace <slug>` 默认只输出服务 principal 与 literal ACL 计划；显式 `--apply` 才按合同更新各服务 SCRAM-SHA-512 凭据并添加/读回核对 ACL。Apply 时管理员连接凭据通过 `KAFKA_SASL_*` 环境变量注入，各服务密码从受保护环境文件读取（`TUBA_INGEST_KAFKA_PASSWORD` 等）；不要把管理员环境导出到服务清单。该命令只添加 ACL，不撤销旧 ACL；source-context Topic 仍由 `tuba-source-topic-admin` 在校验数据库来源登记后单独处理。该命令已在一次性 Kafka 4.3.1 authorizer broker 上完成 apply/重复 reconcile 及 ACL 读回验收。`scripts/verify_kafka_acl_default_deny.sh` 是负向安全验收脚本，不承担身份配置。不要用 24 小时 validation profile 表示生产保留承诺；生产配置仍需 A03 定案。

部署、升级、回滚和集群前置条件见 [`helm/tuba/README.md`](helm/tuba/README.md) 与 [`../docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md)。
