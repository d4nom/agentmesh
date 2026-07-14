.PHONY: up down demo demo-alt chaos test logs lint

up:
	docker compose up -d --build

down:
	docker compose down -v

demo: up
	@echo "Waiting for parser/rag/executor to report healthy..."
	@for i in $$(seq 1 40); do \
		unhealthy=$$(docker compose ps parser rag executor --format '{{.Health}}' 2>/dev/null | grep -vc '^healthy$$'); \
		if [ "$$unhealthy" = "0" ]; then break; fi; \
		sleep 2; \
	done
	uv run python scripts/inject_incident.py --scenario connection_exhaustion
	@echo "Waiting for the plan to land in executor logs..."
	@sleep 5
	docker compose logs --tail=50 executor
	@echo ""
	@echo "Jaeger UI (trace across parser -> rag -> executor): http://localhost:16686"

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
