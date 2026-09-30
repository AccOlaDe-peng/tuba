SHELL := /bin/sh

.PHONY: bootstrap check test build contracts helm-check go-check python-check analysis-evaluate web-check dev-up dev-down topics clean

bootstrap:
	uv sync --project python --frozen
	corepack pnpm@12.6.0 --dir web install --frozen-lockfile

contracts:
	python3 scripts/validate_contracts.py
	python3 scripts/validate_openapi.py
	python3 scripts/generate_es_templates.py

helm-check:
	helm lint deploy/helm/tuba
	helm template tuba deploy/helm/tuba --namespace tuba --set gateway.enabled=true --set observability.serviceMonitor.enabled=true --set observability.prometheusRule.enabled=true --set observability.dashboard.enabled=true > /tmp/tuba-render.yaml

go-check:
	go test ./...
	go vet ./...

python-check:
	uv run --project python python -m unittest discover -s python/tests
	uv run --project python tuba-analysis-evaluate python/scenarios/*.json --min-precision 1 --min-recall 1

analysis-evaluate:
	uv run --project python tuba-analysis-evaluate python/scenarios/*.json --min-precision 1 --min-recall 1

web-check:
	corepack pnpm@12.6.0 --dir web typecheck
	corepack pnpm@12.6.0 --dir web test

check: contracts helm-check go-check python-check web-check

test: check

build:
	go build ./cmd/...
	uv build --project python
	corepack pnpm@12.6.0 --dir web build

dev-up:
	docker compose --profile identity up -d

dev-down:
	docker compose down

topics:
	docker compose run --rm kafka-init

clean:
	rm -rf web/dist python/dist
