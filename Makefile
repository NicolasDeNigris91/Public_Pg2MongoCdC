# Zero-Downtime Migration — operator UX.
# Every target is idempotent. `make demo` followed by `make demo` is a no-op.
# Every chaos/verify target has a PASS criterion printed on success/failure.

SHELL := bash
.ONESHELL:
.DEFAULT_GOAL := help

COMPOSE      := docker compose
CONNECT_URL  := http://localhost:8083
PG_EXEC      := $(COMPOSE) exec -T postgres psql -U app -d app
MONGO_EXEC   := $(COMPOSE) exec -T mongo mongosh --quiet mongodb://localhost:27017/migration?replicaSet=rs0

# --------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

# --------------------------------------------------------------------
# Core lifecycle
# --------------------------------------------------------------------
.PHONY: demo
demo: ## Boot the core stack and register connectors
	@[ -f .env ] || cp .env.example .env
	$(COMPOSE) up -d --build --wait
	bash scripts/register-connectors.sh
	@echo ""
	@echo "Stack is up. Next:"
	@echo "  make seed             # insert test rows"
	@echo "  make show-topics      # see CDC events flowing"
	@echo "  make status           # connector health"

.PHONY: demo-full
demo-full: ## Boot core stack + chaos overlay (Prometheus, Grafana, Toxiproxy, exporters, Jaeger)
	@[ -f .env ] || cp .env.example .env
	$(COMPOSE) -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait
	bash scripts/register-connectors.sh
	@echo ""
	@echo "Full stack is up. Open:"
	@echo "  http://localhost:3000   (Grafana, user=anonymous)"
	@echo "  http://localhost:9090   (Prometheus)"
	@echo "  http://localhost:16686  (Jaeger - end-to-end traces)"
	@echo "  http://localhost:8474   (Toxiproxy admin)"

.PHONY: down
down: ## Stop the stack, keep volumes
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop the stack AND delete all data volumes (destructive)
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs from all services (Ctrl-C to exit)
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

# --------------------------------------------------------------------
# Connector management
# --------------------------------------------------------------------
.PHONY: register-connectors
register-connectors: ## Register Debezium (source) + MongoDB (sink) connectors
	bash scripts/register-connectors.sh

.PHONY: status
status: ## Show connector status
	@curl -fsS $(CONNECT_URL)/connectors?expand=status | jq '.[] | {name: .status.name, state: .status.connector.state, tasks: [.status.tasks[].state]}'

.PHONY: unregister-connectors
unregister-connectors: ## Delete all registered connectors
	@for c in $$(curl -fsS $(CONNECT_URL)/connectors | jq -r '.[]'); do \
		echo "Deleting $$c"; curl -sS -X DELETE $(CONNECT_URL)/connectors/$$c; \
	done

# --------------------------------------------------------------------
# Data paths
# --------------------------------------------------------------------
.PHONY: seed
seed: ## Insert small burst of rows into Postgres; verify they land in Mongo
	bash scripts/seed.sh

.PHONY: show-topics
show-topics: ## List Kafka topics and recent CDC events
	bash scripts/show-topics.sh

# --------------------------------------------------------------------
# Load, chaos, verification
# --------------------------------------------------------------------
.PHONY: load
load: ## Run k6 load test against Postgres (requires docker-compose.chaos.yml up)
	# MSYS_NO_PATHCONV stops Git Bash on Windows from rewriting /scripts/...
	# into C:/Program Files/Git/scripts/... before the docker invocation.
	MSYS_NO_PATHCONV=1 $(COMPOSE) -f docker-compose.yml -f docker-compose.chaos.yml run --rm k6 run /scripts/write-mix.js

.PHONY: load-slo
load-slo: ## Run k6 then assert CDC-side SLOs (lag p99, error rate) against Prometheus. Fails build on breach.
	MSYS_NO_PATHCONV=1 $(COMPOSE) -f docker-compose.yml -f docker-compose.chaos.yml run --rm k6 run /scripts/write-mix.js
	bash scripts/check-load-slos.sh

.PHONY: check-load-slos
check-load-slos: ## Run only the post-load SLO assertions against the live Prometheus
	bash scripts/check-load-slos.sh

.PHONY: chaos
chaos: ## Run all chaos scenarios, fail on any failure
	bash chaos/run-all.sh

.PHONY: verify
verify: ## Compare PG vs Mongo row counts and content hashes
	bash chaos/verify-integrity.sh

.PHONY: check-alerts
check-alerts: ## Verify every alert rule has a producing metric in code (drift detector)
	bash scripts/check-alert-metrics.sh

.PHONY: reprocess-dlq
reprocess-dlq: ## Triage DLQ topics (dry-run). Pass ARGS="--replay" to actually re-publish.
	bash scripts/reprocess-dlq.sh $(ARGS)
