# Batch Analytics Pipeline — operator commands.
# Every target runs against the dockerised stack. `make up` first, then the rest.

COMPOSE := docker compose
RUN     := $(COMPOSE) run --rm web python manage.py
EXEC    := $(COMPOSE) exec web python manage.py

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

.PHONY: up
up: env ## Build images and start the whole stack (postgres, clickhouse, localstack, web)
	$(COMPOSE) up -d --build
	@echo "Stack is starting. Web API will be at http://localhost:8000"

.PHONY: down
down: ## Stop the stack
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete all volumes (DESTRUCTIVE)
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail web logs
	$(COMPOSE) logs -f web

.PHONY: migrate
migrate: ## Apply Django migrations (Postgres schema)
	$(EXEC) migrate --noinput

.PHONY: init-clickhouse
init-clickhouse: ## Create ClickHouse databases, tables and reporting views
	$(EXEC) init_clickhouse

.PHONY: bootstrap-s3
bootstrap-s3: ## Create the S3 bronze bucket + Firehose delivery stream in LocalStack
	$(EXEC) bootstrap_s3

.PHONY: seed
seed: ## Idempotently load the CSV dataset into Postgres (system of record)
	$(EXEC) seed

.PHONY: run-batch
run-batch: init-clickhouse bootstrap-s3 ## Run one incremental batch: Postgres -> S3 Bronze (Parquet) -> ClickHouse Silver
	$(EXEC) run_batch

.PHONY: backfill
backfill: init-clickhouse bootstrap-s3 ## Reprocess everything from scratch (resets watermarks). Usage: make backfill [ARGS="--from 2026-01-01 --to 2026-01-31"]
	$(EXEC) backfill $(ARGS)

.PHONY: pipeline
pipeline: seed run-batch ## Convenience: seed then run one batch end-to-end

.PHONY: test
test: ## Run the test suite (idempotency, reruns, metric calculations)
	$(COMPOSE) run --rm -e DJANGO_SETTINGS_MODULE=config.settings web pytest -q

.PHONY: dashboard
dashboard: ## Open the verification dashboard URL
	@echo "Dashboard:        http://localhost:8000/"
	@echo "Daily metrics:    http://localhost:8000/api/metrics/daily?start=2026-01-01&end=2026-01-31"
	@echo "Summary metrics:  http://localhost:8000/api/metrics/summary"

.PHONY: shell-ch
shell-ch: ## Open a ClickHouse SQL client
	$(COMPOSE) exec clickhouse clickhouse-client
