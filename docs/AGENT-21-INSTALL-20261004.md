# 21 主机 Management Agent 安装记录

2026-10-04 在 Zeek 采集主机 `10.6.69.21` 安装当前仓库构建的 Linux amd64 `tuba-agent`，通过租户管理员 API 创建短时一次性 enrollment 并完成注册。令牌经 SSH 标准输入传送，未记录明文；安装身份保存在权限受限的目录。

- 主机显示名称：`zeek-10.6.69.21`。
- 管理端：`http://10.6.68.248:8088`，现有私网开发入口。
- 安装根目录：`/opt/tuba/management-agent`。
- 程序：`bin/tuba-agent`；状态：`state/`；日志：`logs/agent.log`；运行 PID：`agent.pid`。
- 列表验证：state=running、online=true，首次心跳北京时间 19:54:59。
- 现有 Filebeat 进程未停止、重启或重复启动。

## 当前范围

注册、心跳和配置轮询已运行。当前 CLI 创建的 Runner 未接入 Supervisor 和来源状态提供器，因此此安装尚不展示 Filebeat 的采集状态，也不执行组件接管或升级。不得将管理在线等同于采集组件健康。没有注册系统服务，当前进程通过 nohup 启动；主机重启后需显式恢复运行。

Windows 来源主机及其他采集主机未安装本次 Agent，不应认为全部采集端已受管。

## 本机检查与重新启动

```sh
/opt/tuba/management-agent/bin/tuba-agent status --state-dir /opt/tuba/management-agent/state
```

重新启动前须检查 `agent.pid` 指向的进程是否仍为该安装目录下的 Agent，避免启动第二个进程。确认未运行后执行：

```sh
nohup /opt/tuba/management-agent/bin/tuba-agent run \
  --api-url http://10.6.68.248:8088 \
  --state-dir /opt/tuba/management-agent/state \
  >> /opt/tuba/management-agent/logs/agent.log 2>&1 < /dev/null &
echo $! > /opt/tuba/management-agent/agent.pid
```

停用可在前端执行，或核对进程身份后发出 SIGTERM。前端禁用的身份需要通过管理 API 恢复，不能通过篡改本地状态文件恢复授权。
