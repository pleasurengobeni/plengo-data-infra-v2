# =============================================================================
# Makefile — wasac_analytics stack management
#
# Always sources ~/.airflow before any docker compose command so real env vars
# are available regardless of how the command is run.
#
# Usage:
#   make            → start stack (same as: make up)
#   make up
#   make down
#   make restart
#   make logs
#   make restart-service s=web-ui
#   make logs-service   s=web-ui
# =============================================================================

SHELL       := /bin/bash
ENV_FILE    := .env
AIRFLOW_ENV := $(HOME)/.airflow

# Load ~/.airflow if it exists, then run docker compose
DC = source $(AIRFLOW_ENV) 2>/dev/null && docker compose --env-file $(ENV_FILE)

.PHONY: up down restart logs restart-service logs-service ps

up:
	@echo "▶  Sourcing $(AIRFLOW_ENV) ..."
	@$(DC) up -d
	@echo ""
	@echo "✓  Stack started"
	@echo "   Airflow           → http://localhost:8090"
	@echo "   Web UI            → http://localhost:5001"
	@echo "   Pipeline Monitor  → http://localhost:8050"
	@echo "   Jenkins           → http://localhost:9090"
	@echo "   PgAdmin           → http://localhost:5050"
	@echo "   DAG Builder       → docker compose logs -f dag-builder"

down:
	@echo "▶  Sourcing $(AIRFLOW_ENV) ..."
	@$(DC) down
	@echo "✓  Stack stopped"

restart:
	@echo "▶  Sourcing $(AIRFLOW_ENV) ..."
	@$(DC) down
	@$(DC) up -d
	@echo "✓  Stack restarted"

logs:
	@$(DC) logs -f

## Restart a single service:  make restart-service s=web-ui
restart-service:
	@echo "▶  Sourcing $(AIRFLOW_ENV) ..."
	@$(DC) restart $(s)
	@echo "✓  $(s) restarted"

## Tail logs for one service:  make logs-service s=airflow-scheduler
logs-service:
	@$(DC) logs -f $(s)

ps:
	@$(DC) ps
