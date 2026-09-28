# Kubernetes/Helm 部署参考

> 本文保留早期 Kubernetes/Helm 方案供后续扩展评估，不是当前单节点部署手册。当前产品目标是各组件单实例，由产品 Launcher/CLI 管理进程；Collector 使用跨 Linux/Windows 统一目录包并由自身 CLI 管理，不注册 systemd 或 Windows Service。以[完整目标架构第 11 章](TARGET-ARCHITECTURE.md#11-单节点部署与运维)和 [IMPLEMENTATION-TODO.md](IMPLEMENTATION-TODO.md) 为准。

## 目标环境

- Kubernetes 1.36.x，Helm 4.3.x
- Kubernetes Gateway API + Envoy Gateway 1.9.x
- Kafka 4.3.x：至少 3 broker，RF=3，`min.insync.replicas=2`，TLS/SASL
- PostgreSQL 18.6：受管实例或主备拓扑，生产连接启用 TLS
- Elasticsearch 8.19.x：至少 3 个数据节点，索引副本满足故障域要求
- Keycloak 26.7.x 或企业 OIDC
- Vault 1.21.x 和 Secrets Store CSI Driver
- Prometheus、Alertmanager、Grafana 和可选 Kafka Exporter

开发 Compose 仅用于本地验证，不满足生产 HA、TLS、ACL 或 RPO/RTO 要求。

## 密钥

运行时 Secret `tuba-runtime` 至少包含：

| Key | 用途 |
| --- | --- |
| Source API key | 单个已登记来源的 ingest 凭证；在来源注册时一次性签发 |
| `ES_API_KEY` | 各服务受限 Elasticsearch 凭据 |
| `DATABASE_URL` | API 和分析 worker 的 PostgreSQL DSN |
| `KAFKA_SASL_USERNAME` | Kafka SASL 用户 |
| `KAFKA_SASL_PASSWORD` | Kafka SASL 密码 |

当 `common.kafka.tls.existingSecret` 非空时，chart 将该 Secret 以只读方式挂载到所有服务，并设置 `KAFKA_TLS_CA_FILE`、`KAFKA_TLS_CERT_FILE` 和 `KAFKA_TLS_KEY_FILE`。

生产优先通过 Vault CSI 同步为 Kubernetes Secret。启用 `vault.enabled=true` 后，chart 创建 `SecretProviderClass`，工作负载以只读卷挂载，并通过同步 Secret 注入环境变量。密钥不得写入镜像、values、Git、日志或 DLQ。

## 部署

先构建并推送锁定镜像：

```powershell
.\scripts\build_images.ps1 -Registry registry.example.com/tuba -Tag 1.0.0 `
  -OIDCIssuer https://identity.example/realms/tuba -Push
```

```bash
cp deploy/helm/tuba/values-production.yaml.example deploy/helm/tuba/values-production.yaml
helm lint deploy/helm/tuba
helm upgrade --install tuba deploy/helm/tuba \
  --namespace tuba \
  --create-namespace \
  --values deploy/helm/tuba/values-production.yaml \
  --atomic \
  --wait \
  --timeout 10m
```

Windows 可使用：

```powershell
.\scripts\deploy_helm.ps1
```

## 滚动升级

Deployment 使用 `maxUnavailable: 0` 和 `maxSurge: 1`。每个服务配置 liveness/readiness、PDB、HPA、反亲和和 zone topology spread。

上线前验证：

1. 新镜像通过签名、SBOM 和漏洞扫描。
2. 数据库 migration 已前滚并完成备份。
3. Kafka topic、ACL 和消费者组配额已检查。
4. `helm upgrade --atomic --wait` 无超时。
5. 观察至少 30 分钟错误率、Kafka lag、ES bulk 失败和 API P95。

## 回滚

```bash
helm history tuba --namespace tuba
helm rollback tuba <revision> --namespace tuba --wait
```

或：

```powershell
.\scripts\rollback_helm.ps1 -Revision <revision>
```

回滚应用镜像不会自动回滚数据库。数据库变更必须向前兼容，破坏性变更需要单独的前滚修复 migration。

## 网络策略

chart 默认创建 NetworkPolicy，限制：

- Gateway 只能访问 `api` 和 `web`。
- 同 release Pod 可以访问 metrics 端口。
- 服务只能向 DNS、Kafka、PostgreSQL、Elasticsearch、Keycloak 和 OTel Collector 发起必要连接。

`0.0.0.0/0` 是兼容不同集群拓扑的初始值。生产应替换为中间件 CIDR、namespace selector 或明确 egress gateway。
