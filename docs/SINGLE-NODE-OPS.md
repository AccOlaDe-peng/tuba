# 单节点操作手册（install / upgrade / rollback / uninstall）

适用范围：248 式单节点数据面（Launcher 监督的 19 个数据面服务 + 5 个监控服务）。
Kubernetes/Helm 形态见 `DEPLOYMENT.md`；事故处置见 `RUNBOOK.md`（本文不重复排障流程）。
所有操作先在 21 或本地归档旧资产并记录 sha256，再变更；明文凭据绝不进仓库/文档/提交信息。

## 1. 安装（install）

前置：宿主机已装 ADMS 提供的 Kafka（`/opt/adms/kafka`，broker 10.6.68.248:29292）、
PostgreSQL（`/opt/adms/postgresql`）、Elasticsearch（127.0.0.1:9200）；已建 `tuba` 系统用户。

1. 控制面 schema：按 `migrations/` 序号经 `scripts/apply_postgres_migrations.sh` 应用；
   台账在 `public.tuba_schema_migrations`。**远程应用时必须拆 Up-only 文件**
   （`\i` 会把 goose Down 段一并执行，00020/00021 各踩过一次）。
2. ES 资产：`scripts/apply_elasticsearch_assets.sh` 应用 `elasticsearch/` 下的
   ILM/组件模板/索引模板（API Key 经环境变量注入）。
3. Kafka：按命名空间建 topic（raw / events / quarantine / dlq / attributed /
   analysis.results.v2），SCRAM 身份与 typed principal（`User:<name>`）ACL 就位——
   裸用户名 ACL 会静默匹配为空。
4. 二进制：`install -m 0750 -o root -g tuba` 到 `/opt/tuba/bin/`；Python 分析面在
   `/opt/tuba/analysis/`（venv + `PYTHONPATH=/opt/tuba/analysis` 运行包源码）。
5. 服务清单：`/etc/tuba/tuba-services.json` 声明服务、环境变量、metrics 端口；
   端口不得与既有链冲突。密钥经 `${VAR}` 占位从 `/etc/tuba/tuba.env`（0600）注入。
6. 启动：`tuba-launcher run --manifest /etc/tuba/tuba-services.json`（首次需
   manifest-wide 启动让监督器读清单）。验证：`status` 全 running、
   各 `/health/ready` 200、消费组 lag 归零、ES 各域计数增长。

## 2. 升级（upgrade）

**黄金路径（单服务粒度）**：

```
sha256sum 新二进制（本地/248 两处一致）
cp -a /opt/tuba/bin/<svc> /opt/tuba/bin/<svc>.pre-<变更标签>-<日期>   # 就地归档
install -m 0750 -o root -g tuba <新二进制> /opt/tuba/bin/<svc>
/opt/tuba/bin/tuba-launcher restart --manifest /etc/tuba/tuba-services.json --service <svc>
```

- `--manifest` 必填；`--service` 粒度，其余服务不动。归档同时异地一份到
  21 `/opt/tuba-backup/248/<日期>-<标签>/`（248→21 免密 key `/root/.ssh/tuba_backup_ed25519`）。
- 新增/删除清单服务需要 manifest-wide restart（运行中的监督器不重读清单）——
  属窗口操作，先确认无大批量积压。
- Python 分析面升级：替换 `/opt/tuba/analysis/tuba_analysis/*.py`（旧文件就地
  `.pre-<标签>` 归档、清 `__pycache__`、sha256 与仓库工作区一致），
  然后 `restart --service zeek-analysis-worker --service tenant-a-analysis-worker`。
- Web：`vite build --base=/tuba/` → 解包到 `/opt/tuba/web/dist-<标签>` →
  `ln -sfn` + `mv -T` 原子切软链（旧 dist 目录保留即回滚）。ADMS 网关无需重启。
- Launcher 退避 1→30s，稳定 5 分钟重置；升级后观察 restarts 计数不增、
  日志无新 ERROR、消费组 lag 追平，才算完成。

## 3. 回滚（rollback）

1. 二进制级：还原 `.pre-*` 归档（或 21 异地归档，`sha256sum -c` 复核）→
   同一条 `restart --service`。
2. 清单级：`/etc/tuba/tuba-services.json.pre-<标签>` 还原 + manifest-wide restart。
3. Web：软链指回旧 `dist-<标签>`。
4. 数据面兼容纪律：事件/投影按 revision 幂等，回滚后重放不丢不重；
   涉及 PG schema 的升级须为 expand-only（见下），保证旧二进制可读。

## 4. 卸载（uninstall）

**默认保留数据**（Kafka topic、ES 索引、PG 表、备份），只停服务：

```
/opt/tuba/bin/tuba-launcher stop --manifest /etc/tuba/tuba-services.json
```

之后按需删除 `/opt/tuba/bin`、`/opt/tuba/analysis`、web dist、清单与 env。
数据销毁是单独的显式操作（删 topic / 删索引 / drop schema），必须另有工单授权，
绝不随卸载发生。

## 5. 迁移策略：expand / migrate / contract

- **expand**：新列/新表/新索引先以兼容旧代码的方式上线（可空、有默认值）；
- **migrate**：双写或回填在新版本全部就绪后执行（回填任务属 B01 范围，
  固定输入范围与输出状态空间）；
- **contract**：确认无旧版本在读旧形状后才删旧列/旧索引。
- 影子 generation 切换（新旧代次并行、追平水位、PG 激活指针）属 B02 范围，
  未实现前不做跨代次双写。
