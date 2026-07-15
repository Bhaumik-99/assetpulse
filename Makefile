.PHONY: install download-data ingest validate transform dbt-run features test lint benchmark dashboard pipeline docker-up docker-down clean

install:
	pip install -e ".[dev]"

download-data:
	python scripts/download_dataset.py

seed-data:
	python scripts/seed_data.py

ingest:
	python -m src.ingestion.cmapss_ingestor

validate:
	python -m src.validation.quality_runner

transform:
	python -m src.transformations.bronze_to_silver

dbt-run:
	cd dbt/assetpulse && dbt run

features:
	python -m src.transformations.feature_engineering

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v -m integration

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

lint-fix:
	ruff check --fix src/ tests/
	ruff format src/ tests/

benchmark:
	python benchmarks/pandas_vs_polars.py

dashboard:
	streamlit run dashboard/app.py

pipeline:
	python -m src.ingestion.cmapss_ingestor
	python -m src.validation.quality_runner --layer bronze
	python -m src.transformations.bronze_to_silver
	python -m src.validation.quality_runner --layer silver
	python -m src.transformations.feature_engineering
	python -m src.transformations.health_metrics
	@echo "Pipeline complete."

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

clean:
	rm -rf data/bronze/ data/silver/ data/gold/ data/quarantine/ data/metadata/
	rm -rf logs/*.log logs/*.json
	rm -rf .pytest_cache .ruff_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
