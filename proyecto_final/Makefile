.PHONY: up down logs test lint

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f consumer producer

test:
	pytest tests/ -v --tb=short

lint:
	pre-commit run --all-files
