# 248 数据面非 root 转换方案

> 状态：**已执行（2026-10-01 维护窗口）**。对应 [IMPLEMENTATION-TODO.md](IMPLEMENTATION-TODO.md) O01 接管后遗留第 2 条（已勾选）。执行记录与实测差异见文末「执行结果」。
>
> 侦察方法：经 SSH 对 248 全程只读（`ls/stat/find/getent/id/readlink`/`/proc`/`ss`），未读取任何密钥内容（`tuba.env`/`secrets.json`/`admin.properties` 只看权限位与属主）。

## 1. 现状权限清单（2026-10-01 实测）

### 账号

| 账号 | uid:gid | shell | 用途 | 现状 |
| --- | --- | --- | --- | --- |
| `tuba` | 967:965 | `/usr/sbin/nologin` | 数据面服务账号（已预建） | 仅属自身组，无家目录内容（home 指向 `/var/lib/tuba`），当前**无任何进程**使用 |
| `tuba-prometheus` | 971:969 | nologin | 监控 | 经 setpriv 运行 |
| `tuba-node-exporter` | 970:968 | nologin | 监控 | 经 setpriv 运行 |
| `tuba-kafka-exporter` | 969:967 | nologin | 监控 | 经 setpriv 运行 |
| `tuba-grafana` | 968:966 | nologin | 监控 | 经 setpriv 运行 |

两个 Launcher 主进程均为 **root**：pid 1842144（数据面 `tuba-services.json`，旧映像，无 `service_control` 能力）与 pid 1989059（监控栈 `tuba-monitoring.json`，新映像）。11 个数据面服务全部以 root 运行（继承 Launcher）。

### 路径 × 属主 × 权限 × 需求方矩阵

| 路径 | 属主 | 权限 | 内容 | 谁需要读 | 谁需要写 |
| --- | --- | --- | --- | --- | --- |
| `/opt/tuba` | root:root | 0755 | 顶层 | 全部服务（cwd） | root |
| `/opt/tuba/bin` | root:root | 0755 | 数据面 `tuba-api` 与运维 CLI | api 服务 | root |
| `/opt/tuba/bin/tuba-api` | root:root | **0750** | api 二进制 | **api（目前靠 root）** | root |
| `/opt/tuba/bin/tuba-launcher` | root:root | 0755 | Launcher | 仅 root（两个 Launcher 均 root） | root |
| `/opt/tuba/bin/tuba-launcher.sha256` | root:root | 0640 | 校验 sidecar | root | root |
| `/opt/tuba/bin/tuba-boot` | root:root | 0700 | @reboot 入口 | 仅 root（crond） | root |
| `/opt/tuba/bin/tuba-{ingest,indexer,analysis-sink,…}` | root:root | 0750 | 其余运维/组件二进制 | 当前 248 清单未用；转换后 tuba 组需读 | root |
| `/opt/tuba/collector-live` | root:root | **0700** | 数据面树根 | **全部 10 个 pipeline 服务（目前靠 root）** | root |
| `…/pipeline/bin/tuba-*` | root:root | 0750 | 10 个组件二进制 + 历史 .bak | 全部 10 个服务 | root |
| `…/pipeline/config/source-adapter.json` | root:root | 0640 | adapter 绑定配置（无密钥） | zeek-source-adapter（实测有打开 fd） | root |
| `…/pipeline/run/` `…/pipeline/logs/` | root:root | 0750/文件 0600 | **旧监督器遗物**（pid/日志），Launcher 不使用 | 无人（仅回滚退路用，回滚时是 root） | root |
| `…/collector-live/kafka/` | root:root | 0700（secrets.json 0600） | **隔离 Kafka broker 的配置/数据/SCRAM**，broker 以 root 运行（java pid 1720538，监听 10.6.68.248:29292） | 仅 broker(root) 与运维 | broker(root) |
| `…/capacity-guard/` | root:root | 0750 | capacity-guard 脚本与日志 | capacity-guard（root） | capacity-guard（root） |
| `/etc/tuba` | root:root | **0700** | 配置目录 | Launcher(root) | root |
| `/etc/tuba/tuba-services.json` | root:root | 0600 | 数据面清单（密钥均为 `${…}` 引用） | Launcher(root) | root |
| `/etc/tuba/tuba.env` | root:root | **0600** | **密钥文件**（DATABASE_URL/ES_API_KEY/Kafka SCRAM/token 等） | **仅 Launcher(root)**——注入子进程环境 | root |
| `/etc/tuba/tuba-monitoring.json` / `tuba-monitoring.env` | root:root | 0600 | 监控清单/密钥 | 监控 Launcher(root) | root |
| `/etc/tuba/tuba.env.bak-*`、`runtime.env`、`kafka-client.properties` 等 | root:root | 0600 | 历史备份/旧文件 | 无人（留档） | root |
| `/var/lib/tuba/launcher{,-monitoring}/` | root:root | 0700（state 0600） | Launcher state（含 runner_identity） | 仅 Launcher(root) | Launcher(root) |
| `/var/log/tuba/` | root:root | 0700（日志 0600） | 数据面 11 服务日志 + boot.log | 写入方是 **Launcher(root)**（子进程 stdout 继承 fd） | Launcher(root) |
| `/var/log/tuba-monitoring/` | root:root | 0700 | 监控栈日志 | 同上 | Launcher(root) |
| `/opt/tuba/monitoring/` | root:root | 0755；`data/<comp>` 与 `logs/grafana` 属各 tuba-* 账号 0750 | 监控栈 | 各组件经 setpriv 读二进制/配置（root 属主 world-readable 足够） | 各 tuba-* 账号写自己的 data/logs |
| `/opt/tuba/run/` | root:root | 0755 | 旧 pid 遗物 | 无人 | root |
| `/opt/tuba/backup*`、`logs/`、`migrations*`、`scripts/` | root:root | 0600–0750 | 运维脚本（root cron 执行） | 仅 root | root |

### 关键实测结论

1. **11 个数据面服务对磁盘只有读需求，零写需求。** 逐一核对 `/proc/<pid>/fd`：唯一打开的本机文件是 source-adapter 读的 `pipeline/config/source-adapter.json`；日志由 root Launcher 以已打开的 fd 喂给子进程；state 只由 Launcher 写；offset/receipt 在 Kafka/PG。工作目录均为 `/opt/tuba`（0755，无需改）。
2. **所有监听端口均为 loopback 且 >1024**（8080/8788/13000/19090/19095/19096/19100–19102/19185/19295/19395），**不需要 CAP_NET_BIND_SERVICE 或任何 capability**。注意：quarantine/standard indexer 的 `*_METRICS_LISTEN` 在清单里有配置但 `ss` 未观察到对应监听——转换验收时顺带核实（可能是既有行为差异，与身份无关）。
3. **凭据文件不需要改属主。** `tuba.env`（0600 root）由 root Launcher 读取并注入子进程环境；子进程不读文件本身。混合模型下它对 tuba 保持不可读——**影响面为零，不动**。同理 `secrets.json`、`tuba-monitoring.env` 不动。（注意：注入后密钥存在于各 tuba 子进程的 `/proc/<pid>/environ`，同 uid 进程互可读；11 个服务本就共享这些密钥，不扩大暴露面。）
4. **监控栈已是非 root**（setpriv 先例，2026-09-28 容器验收 + 2026-10-01 生产），其读路径全靠 root 属主 world-readable，本次不动。
5. **Kafka broker（29292）是领养的基础设施，以 root 运行**，其数据/密钥在 `collector-live/kafka/`（0700 root）。转换只放开 `collector-live` 本层到 0750 root:tuba，`kafka/` 子目录保持 0700 root——broker 不受影响，tuba 也进不去。
6. ES（elasticsearch 用户）、PG（postgres 用户）是另一产品的实例，不在本方案范围。

## 2. 身份模型决策：Launcher 保 root + 数据面服务降 tuba（混合模型）

**结论：与监控栈同一模式**——Launcher 以 root 运行，11 个数据面服务的清单 `command` 改为 `/usr/bin/setpriv --reuid=967 --regid=965 --clear-groups --no-new-privs <真实二进制>`。

理由：

- Launcher 需要 root 才能 setpriv 降权（监控栈已验证该模式：exec 语义、PID 不变、SIGTERM 直达服务）。若 Launcher 自身降 tuba，则**所有**服务只能是 tuba，未来任何需要其他身份（或 capacity-guard 这类 root）的服务都无法纳管。
- 248 是扁平布局（已定方向：安装器对齐现实、不重装），安装器的"Launcher 以 tuba 运行"模型不照搬。
- Launcher 保持 root 意味着 state/log/env 文件维持 root 0700/0600，**密钥文件权限模型零变化**。
- `no_new_privs` 保证降权不可逆；tuba 是 nologin 系统账号，无非服务入口。

`capacity-guard` 保持 root（需写 ES 集群设置，且当前即 root）。`tuba-boot`（0700 root、cron @reboot）不变。

## 3. 可提前做的无害准备（**列出，未执行**）

1. `id tuba` / `getent passwd tuba` 复核（uid=967、gid=965、nologin、无特权组）——已在本轮侦察确认。
2. 生成候选清单：复制 `tuba-services.json` 为 `/etc/tuba/tuba-services.json.nonroot-candidate`（0600 root），11 个服务的 `command` 逐一套 setpriv；用 `tuba-launcher validate --manifest <候选>` 预检（validate 是只读的，不碰运行态）。
3. 归档现状：`/etc/tuba/tuba-services.json`、两个 launcher-state.json、本方案的路径权限快照（`find /opt/tuba /etc/tuba -printf '%u:%g %m %p\n'`）推送到 `21:/opt/tuba-backup/248/<stamp>/` 留作回滚对照。
4. 可选顺带项：数据面 Launcher 主进程换新版二进制（解决遗留第 4 条能力门）——转换反正要重启主进程，**一次窗口同时完成两件事**，避免第二次整清单重启。需先按既有流程构建/校验/归档旧二进制。

## 4. 转换步骤（维护窗口内执行；预计数据面停机 2–5 分钟，Kafka 12h 保留缓冲，不丢数据）

顺序要点：**先放权（只读路径 chgrp/chmod，运行中的 root 进程不受影响），再换清单重启**。

1. 窗口确认：根盘 <75%、11/11 running、restarts=0、各消费组 lag 低；记录基线（消费组 lag/位点、ES alias 计数、DLQ 末端 offset——可复用 `scripts/o04_reboot_acceptance.sh baseline`）。
2. 放权（**不需要停服务**，root 进程对权限放宽无感）：
   - `chgrp tuba /opt/tuba/collector-live && chmod 0750 /opt/tuba/collector-live`
   - `chgrp -R tuba /opt/tuba/collector-live/pipeline`；目录 0750、二进制 0750、`config/source-adapter.json` 0640（`pipeline/run`、`pipeline/logs` 旧遗物可保持 root，或一并 chgrp，无运行影响）
   - `chgrp tuba /opt/tuba/bin/tuba-api …`（`bin/` 内组件二进制 0750 root:tuba；**`tuba-launcher`、`tuba-boot` 保持 root 属主不动**，避免 tuba 身份能操控 Launcher）
   - **不动**：`/etc/tuba/*`（含 tuba.env）、`/var/lib/tuba`、`/var/log/tuba*`、`collector-live/kafka`、`capacity-guard`、`monitoring`。
3. 停数据面：`tuba-launcher stop --manifest /etc/tuba/tuba-services.json`（旧映像也支持 manifest-wide stop）。
4. 备份原清单后替换为候选清单（0600 root 不变）。
5. （可选同窗口）替换 Launcher 二进制为新映像，`sha256sum -c` 复核。
6. 启动：`tuba-launcher start --manifest /etc/tuba/tuba-services.json`。
7. 验收（下节）。全程监控栈清单不碰。

## 5. 验收清单（转换后逐项）

- [ ] 11/11 running，`restarts` 从 0 起计且稳定期不再增长
- [ ] **逐服务身份**：`ps -o user= -p <pid>` 或 `readlink /proc/<pid>/exe` + `/proc/<pid>/status` 的 `Uid:` 行 = 967/967，且 `NoNewPrivs: 1`；两个 Launcher 仍为 root
- [ ] 端口：8080/8788/19185/19095/19295/19096/19395 等 loopback 监听齐全（顺带核实 quarantine/standard indexer 的 metrics 端口基线差异）
- [ ] 消费组集合与基线一致，活跃组 lag 追平（无成员孤儿组不计）
- [ ] ES 各 raw/domain/quarantine alias 计数 ≥ 基线且恢复增长；DLQ 三 topic 末端 offset 零新增
- [ ] api `/metrics` 200、`/api/v1/*` 401；ingest readiness 200；adapter `/metrics` 计数继续增长
- [ ] Prometheus 6/6 targets UP；kafka-exporter/grafana/node-exporter 身份未变（971/970/969/968）
- [ ] 权限复核：tuba 对 `tuba.env`/`secrets.json`/kafka 数据目录仍无任何访问（`sudo -u tuba test -r` 为否）；对二进制/配置可读可执行
- [ ] 手动执行 `/opt/tuba/bin/tuba-boot` no-op 退出 0（幂等路径未受清单更换影响）

## 6. 回滚步骤

1. `tuba-launcher stop --manifest /etc/tuba/tuba-services.json`
2. 还原备份清单（服务不再套 setpriv），`tuba-launcher start`——服务回到 root 运行。
3. 权限放宽（root:tuba 0750/0640）**无需回收**：这些路径不含密钥，tuba 组唯一成员是 tuba 账号本身，放宽不构成暴露；如坚持还原，按第 3 节归档的权限快照逆向 chgrp/chmod。
4. 若连 Launcher 都不正常，走既有退路：旧 Python 监督器（`manage_zeek_live_pipeline.py` / `tenant_a_chain.py`，先起 api），回退前 `pgrep -af collector-live/pipeline/bin` 核对无孤儿。

## 7. 遗留风险与说明

- 同一 uid（tuba）的 11 个服务可通过 `/proc/<pid>/environ` 互读注入的共享密钥——与现状（共享 root）相比已大幅收窄，且密钥本来就是共享值；逐服务独立身份属更大的改造，不在本条范围。
- 清单更换需要整份数据面清单重启一次（Launcher 无运行期 reload），这是本转换唯一的停机点。
- 转换后 `tuba-launcher status/logs/stop/start` 等运维命令仍由 root 执行，运维流程不变。

## 8. 执行结果（2026-10-01 维护窗口实测）

按第 4 节步骤执行，实测差异：

- **停机时长约 10 秒**（05:51:19–29Z：stop 优雅回收 11 服务 → 换清单 → start）。
- **无需上传 Launcher 二进制**：磁盘上已是目标版（sha256 `750947ad…f5a4`，`sha256sum -c` 通过），整清单重启即激活新映像（`service_control: true`，能力门顺带关闭遗留第 4 条）。
- **额外收紧一项**：`tuba-launcher` 二进制由 0755 收紧为 0750 root:root（避免 tuba 身份执行 Launcher 操控命令；其写 state 会失败，收紧是纵深防御）。
- 验收清单（第 5 节）全部通过：11/11 uid=967 且 NoNewPrivs=1、Launcher 仍 root、loopback 端口齐全、active 消费组 lag=0、ES 计数恢复增长、DLQ 三 topic 末端与基线一致、tuba 对 `tuba.env`/`secrets.json`/`tuba-monitoring.env` 均 DENIED、Prometheus 6/6 UP、稳定 10 分钟 restarts=0；转换后重启整机一次后身份与功能依旧（uid=967 ×11）。
- 同窗口顺带完成 O04 两次真实整机重启验收（见 IMPLEMENTATION-TODO「O04 目标机重启验收执行记录」），并修复 tuba-boot 幂等探测缺陷、补 29292 broker 开机入口。
- 第 5 节中"quarantine/standard indexer metrics 端口未观察到监听"在转换前后一致，确认为与身份无关的既有现象，转入遗留观察项。
