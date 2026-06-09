include .env
export

.PHONY: help build build-dbt build-jupyter up down ps

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  up-core       Start core services (postgres, minio, kafka, spark, trino, hms)"
	@echo "  up-explore    Start JupyterLab for exploration"
	@echo "  up-datagen    Start data generator"
	@echo "  up-airflow    Start Airflow + dbt"
	@echo "  up-jupyter   Start core + JupyterLab"
	@echo "  up-all        Start everything (core + datagen + explore + airflow)"
	@echo "  down          Stop all containers"
	@echo "  ps             Show running containers"
	@echo ""
	@echo "  build         Build all Docker images"
	@echo "  build-dbt     Build dbt Docker image"
	@echo "  build-jupyter Build JupyterLab image only"
	@echo ""
	@echo "Examples:"
	@echo "  make up-core          # Core services only"
	@echo "  make up-all          # Everything"
	@echo "  docker compose --profile core up -d"
	@echo "  docker compose --profile core --profile datagen --profile explore --profile airflow up -d"

# ─── Build ────────────────────────────────────────────────────
build:
	docker compose --profile core --profile datagen --profile explore --profile airflow build

# ─── Up ─────────────────────────────────────────────────────
up-core:
	docker compose --profile core up -d

up-explore:
	docker compose --profile explore up -d

up-datagen:
	docker compose --profile datagen up -d

up-airflow:
	docker compose --profile airflow up -d

up-all:
	docker compose --profile core --profile datagen --profile explore --profile airflow up -d

up-jupyter:
	docker compose --profile core --profile explore build
	docker compose --profile core --profile explore up -d

build-jupyter:
	docker compose --profile explore build

up:
	docker compose --profile core up -d

# ─── Down ─────────────────────────────────────────────────────
down:
	docker compose --profile "*" down

# ─── Status ───────────────────────────────────────────────────
ps:
	@docker compose ps

# ─── Test ─────────────────────────────────────────────────────
test:
	docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
