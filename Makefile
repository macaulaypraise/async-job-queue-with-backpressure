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
