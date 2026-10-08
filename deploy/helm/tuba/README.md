# TUBA Helm chart

The chart deploys the configured application workloads, Gateway API routing, service accounts, NetworkPolicies, PDBs, HPAs, metrics scraping, alerts, a Grafana dashboard, optional Vault CSI secret injection, and optional backup CronJobs.

## Required cluster components

- Kubernetes 1.36.x and Helm 4.3.x
- Gateway API CRDs and Envoy Gateway 1.9.x when `gateway.enabled=true`
- Prometheus Operator CRDs when ServiceMonitor or PrometheusRule is enabled
- Secrets Store CSI Driver and Vault provider when `vault.enabled=true`
- External or managed Kafka 4.3.x, PostgreSQL 18.6, Elasticsearch 8.19.x

## Install

Build and optionally push all locked images first:

```powershell
.\scripts\build_images.ps1 -Registry registry.example.com/tuba -Tag 1.0.0 `
  -Push
```

```bash
helm upgrade --install tuba ./deploy/helm/tuba \
  --namespace tuba \
  --create-namespace \
  --values ./deploy/helm/tuba/values-production.yaml \
  --atomic \
  --wait \
  --timeout 10m
```

Create the runtime secret before enabling Vault, or let the Vault CSI provider synchronize `tuba-vault-runtime`. Required keys are:

- `ES_API_KEY`
- `DATABASE_URL`
- Source API keys are issued once when a source is registered; store each key in the collector's secret store.
- optional Kafka SASL username/password and TLS material

When `common.kafka.tls.existingSecret` is set, the referenced TLS Secret is mounted read-only into every service and the chart configures the CA, certificate, and key file paths.

## Upgrade and rollback

```bash
helm upgrade tuba ./deploy/helm/tuba --namespace tuba --atomic --wait
helm history tuba --namespace tuba
helm rollback tuba 3 --namespace tuba --wait
```

The application workloads use rolling updates with `maxUnavailable: 0`. PDBs and anti-affinity keep at least one replica available during planned disruption.

## Listener addresses

The application defaults to loopback listeners. The Helm chart explicitly sets `TUBA_ALLOW_NON_LOOPBACK_LISTEN=true` for API, ingest, and the analysis metrics endpoints so Kubernetes probes and Prometheus can reach them through the Pod network. NetworkPolicy restricts metrics ingress to the configured monitoring namespaces. Keep this opt-in unset for local and single-host deployments unless another network namespace must connect; when enabled, listener values may use a wildcard or a Pod IP.
