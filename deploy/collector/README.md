> 2026-09-27：本目录当前包仍为旧自研 Collector，仅供历史运行与迁移排空。目标已改为 Filebeat/Winlogbeat＋TUBA Management Agent；下面命令不会安装或管理 Beat。新包尚待 COL-02/08/09 实现。见 [采集设计 v2](../../docs/COLLECTOR-DESIGN.md)。

# TUBA Collector

The package script creates one ZIP bundle containing the same directory layout and CLI for Linux amd64 and Windows amd64. Extract the bundle on the target host, configure `config/collector.json`, then use `tuba-collector.sh` on Linux or `tuba-collector.ps1` on Windows. The Collector process manages its own detached start, graceful stop, status, restart, and log commands; it does not register with systemd or the Windows Service Control Manager. The SQLite spool stays under the bundle's `data` directory when configured with the sample relative path.

Build the unified package from the product directory with `./scripts/package_collector.ps1 -Tag <version>`. It writes `dist/collector/tuba-collector-<version>.zip`. Package contents are `bin/<os>-amd64/`, `config/collector.json`, and the two platform wrapper scripts. Keep the bundle directory in place after extraction because it owns both configuration and persistent spool data. Provide API keys through environment variables before running the control command.

On Linux, run `chmod +x tuba-collector.sh bin/linux-amd64/tuba-collector` after extracting the ZIP, export the configured API-key environment variables, then run `./tuba-collector.sh start`. On Windows, set the same named environment variables and run `./tuba-collector.ps1 start`. Both wrappers accept the same commands and use the same relative config/data layout. The archive is a portable bundle, not an MSI/DEB installer.

Available controls are `start`, `stop`, `restart`, `status`, `logs`, and `run`. `run` stays attached for foreground operation; the other lifecycle commands communicate through local state files in the spool directory. Startup after a host reboot still needs an OS startup entry that invokes the bundle's `start` command; that entry is not installed by this package yet.

The first adapter reads complete Zeek JSON Lines records from configured `path_glob` patterns. Patterns must include active and renamed files (for example, `conn.log*`) so old handles can be drained. It persists device/inode identity and byte offsets through the SQLite stream cursor. Do not use `copytruncate` for production logs. If a file shrinks behind its cursor, that stream pauses with a source-gap error and does not silently reset. Full detection of a rotated file that disappears outside the configured glob remains unfinished.

Filtering supports exact string equality on dotted JSON paths. `shadow` counts matching records and still forwards them; `drop` advances the file cursor and increments its aggregate counter in the same SQLite transaction. Start with an empty rule map and shadow mode. Drop mode also requires an explicit per-source `allow_drop: true` configuration switch. The first slice does not yet provide a management UI for inspecting counters or releasing rejected HTTP records.

Windows Event Log support, automatic boot-start registration, metrics/health endpoints, capacity alarms, and end-to-end deployment acceptance are not included in this slice. The current adapter reads Zeek JSONL files on either OS; a Windows executable does not yet mean Windows Event Log collection is available.

## Remote management status

The API currently has server-side enrollment, heartbeat, config polling, tenant listing, config publication, and credential disable endpoints. The packaged Collector CLI does **not** yet call those management endpoints: enrollment, remote heartbeat/config application, and binary self-upgrade are follow-up implementation work. Today the source API key, source context, endpoint, and filter configuration are still supplied through the local Collector configuration/environment. Do not treat a published control-plane config as active on an agent until the client-side apply and restart-safe persistence flow is implemented.

The intended agent lifecycle and security boundaries are documented in [COLLECTOR-CONTROL-PLANE.md](../../docs/COLLECTOR-CONTROL-PLANE.md).
