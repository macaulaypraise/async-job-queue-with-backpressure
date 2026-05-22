.PHONY: dev down logs shell check-infra test

dev:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

shell:
	docker compose exec app bash

check-infra:
	@echo "Checking Redis..."
	@docker compose exec redis redis-cli ping
	@echo "Checking Postgres..."
	@docker compose exec postgres psql -U postgres -c "SELECT 1"
	@echo "All infrastructure healthy."

test:
	poetry run pytest tests/ -v

ci:  ## Run the same checks as GitHub Actions CI (local verification)
	poetry run ruff check .
	poetry run mypy app/ --ignore-missing-imports --no-strict-optional
	poetry run pytest tests/ -v


load-test:  ## Start Locust load test UI (navigate to http://localhost:8089)
	docker compose --profile load up locust


migrate:  ## Run database migrations (run after make dev if tables are missing)
	docker compose exec app alembic upgrade head
