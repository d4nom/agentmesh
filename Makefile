.PHONY: up down demo demo-alt chaos test logs lint

up:
	docker compose up -d --build

down:
	docker compose down -v

demo:
	@echo "make demo: not implemented yet (Phase 2)"

demo-alt:
	@echo "make demo-alt: not implemented yet (Phase 3)"

chaos:
	@echo "make chaos: not implemented yet (Phase 3)"

test:
	uv run pytest -m "not e2e"

logs:
	docker compose logs -f

lint:
	uv run ruff check .
	uv run ruff format --check .
