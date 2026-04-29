# ─── crypto-market-data-platform ─────────────────────────────────────────────
# Makefile — single entry point for all dev, test, and infra operations
# Usage: make <target>

.PHONY: help run stop test lint fmt tf-init tf-plan tf-apply tf-destroy \
        docker-build docker-push logs clean

INGESTOR_DIR   := services/ingestor
TF_DIR         := infra/terraform
GCP_PROJECT    := vg-ind-2026
GCP_REGION     := us-central1
IMAGE_NAME     := $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT)/crypto-platform/ingestor
IMAGE_TAG      ?= latest

# ─── Help ─────────────────────────────────────────────────────────────────────
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Local Development ────────────────────────────────────────────────────────
run: ## Start full local stack (ingestor + Pub/Sub emulator) via docker-compose
	docker compose up --build

stop: ## Stop and remove local containers
	docker compose down -v

logs: ## Tail logs from all local containers
	docker compose logs -f

# ─── Testing ──────────────────────────────────────────────────────────────────
test: ## Run all unit tests with coverage report
	cd $(INGESTOR_DIR) && \
		pip install -q -r requirements-dev.txt && \
		pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

test-ci: ## Run tests in CI mode (no install, strict)
	cd $(INGESTOR_DIR) && \
		pytest tests/ -v --tb=short --cov=src --cov-report=xml --no-header

# ─── Linting & Formatting ─────────────────────────────────────────────────────
lint: ## Run ruff linter on ingestor service
	cd $(INGESTOR_DIR) && \
		pip install -q ruff && \
		ruff check src/ tests/

fmt: ## Auto-format ingestor code with ruff
	cd $(INGESTOR_DIR) && \
		pip install -q ruff && \
		ruff format src/ tests/

# ─── Docker ───────────────────────────────────────────────────────────────────
docker-build: ## Build ingestor Docker image (linux/amd64 for Cloud Run)
	docker build \
		--platform linux/amd64 \
		--tag $(IMAGE_NAME):$(IMAGE_TAG) \
		$(INGESTOR_DIR)

docker-push: ## Push ingestor image to Artifact Registry (requires gcloud auth)
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

# ─── Terraform ────────────────────────────────────────────────────────────────
tf-init: ## Initialise Terraform (first run only)
	cd $(TF_DIR) && terraform init

tf-plan: ## Show Terraform execution plan
	cd $(TF_DIR) && terraform plan -var="project_id=$(GCP_PROJECT)"

tf-apply: ## Apply Terraform changes (interactive confirmation required)
	cd $(TF_DIR) && terraform apply -var="project_id=$(GCP_PROJECT)"

tf-destroy: ## DANGER: Tear down all GCP infrastructure
	cd $(TF_DIR) && terraform destroy -var="project_id=$(GCP_PROJECT)"

tf-fmt: ## Format all Terraform files
	cd $(TF_DIR) && terraform fmt -recursive

tf-validate: ## Validate Terraform configuration
	cd $(TF_DIR) && terraform validate

# ─── Cleanup ──────────────────────────────────────────────────────────────────
clean: ## Remove Python cache, coverage, and build artefacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
	find . -name "coverage.xml" -delete 2>/dev/null || true
	@echo "Clean complete."
	
test-beam: ## Run Beam pipeline unit tests
	cd beam && \
		pip install -q -r requirements-dev.txt && \
		pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

beam-run: ## Run Beam DirectRunner locally (requires Pub/Sub emulator running)
	cd beam && BEAM_RUNNER=direct python -m src.pipeline

DBT_DIR := dbt/crypto_platform

dbt-deps: ## Install dbt packages (run once after clone or packages.yml change)
	cd $(DBT_DIR) && dbt deps --profiles-dir .

dbt-compile: ## Compile dbt models (SQL validation — no BigQuery connection needed)
	cd $(DBT_DIR) && dbt compile --profiles-dir . --target dev

dbt-run: ## Run all dbt models against BigQuery
	cd $(DBT_DIR) && dbt run --profiles-dir . --target prod

dbt-run-silver: ## Run Silver layer models only
	cd $(DBT_DIR) && dbt run --select silver --profiles-dir . --target prod

dbt-run-gold: ## Run Gold layer models only
	cd $(DBT_DIR) && dbt run --select gold --profiles-dir . --target prod

dbt-test: ## Run all dbt schema + custom data tests
	cd $(DBT_DIR) && dbt test --profiles-dir . --target prod

dbt-test-silver: ## Run Silver layer tests only
	cd $(DBT_DIR) && dbt test --select silver --profiles-dir . --target prod

dbt-test-gold: ## Run Gold layer tests only
	cd $(DBT_DIR) && dbt test --select gold --profiles-dir . --target prod

dbt-freshness: ## Check source table freshness (requires BigQuery connection)
	cd $(DBT_DIR) && dbt source freshness --profiles-dir . --target prod

dbt-docs: ## Generate and serve dbt documentation locally
	cd $(DBT_DIR) && dbt docs generate --profiles-dir . && dbt docs serve

dbt-clean: ## Remove dbt target and packages directories
	cd $(DBT_DIR) && dbt clean

# ─── Airflow ──────────────────────────────────────────────────────────────────
AIRFLOW_DIR := airflow

airflow-up: ## Start Airflow stack (webserver + scheduler + postgres) via OrbStack
	cp $(AIRFLOW_DIR)/.env.airflow.example $(AIRFLOW_DIR)/.env.airflow 2>/dev/null || true
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml up --build -d
	@echo "Airflow UI: http://localhost:8081 (admin / admin)"

airflow-down: ## Stop and remove Airflow containers and volumes
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml down -v

airflow-logs: ## Tail Airflow scheduler and webserver logs
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml logs -f airflow-scheduler airflow-webserver

airflow-restart-scheduler: ## Restart Airflow scheduler only (picks up new DAG code)
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml restart airflow-scheduler

airflow-shell: ## Open a bash shell in the Airflow scheduler container
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml exec airflow-scheduler bash

test-airflow: ## Run Airflow DAG integrity and operator unit tests
	cd $(AIRFLOW_DIR) && \
		pip install -q apache-airflow==2.10.3 apache-airflow-providers-google \
		            apache-airflow-providers-http dbt-bigquery pytest pytest-cov && \
		pytest tests/ -v --tb=short --cov=. --cov-report=term-missing

airflow-variables-set: ## Bootstrap required Airflow Variables (run after airflow-up)
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml exec airflow-scheduler \
		airflow variables set dead_letter_max_per_hour 100
	docker compose -f $(AIRFLOW_DIR)/docker-compose.airflow.yml exec airflow-scheduler \
		airflow variables set silver_min_rows_per_hour 50
	@echo "Set ingestor_cloud_run_url manually after terraform apply:"
	@echo "  make airflow-shell"
	@echo "  airflow variables set ingestor_cloud_run_url <cloud-run-url>"

dbt-run-moving-averages: ## Run gold_moving_averages model only
	cd $(DBT_DIR) && dbt run --select gold_moving_averages --profiles-dir . --target prod

dbt-test-moving-averages: ## Run tests for gold_moving_averages model
	cd $(DBT_DIR) && dbt test --select gold_moving_averages --profiles-dir . --target prod