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
	docker compose exec parser python scripts/inject_incident.py --scenario connection_exhaustion
	@echo "Waiting for the plan to land in executor logs..."
	@sleep 5
	docker compose logs --tail=50 executor
	@echo ""
	@echo "Jaeger UI (trace across parser -> rag -> executor): http://localhost:16686"

demo-alt:
	SYSTEM_CONFIG_FILE=triage_with_summary.yaml docker compose --profile with-summary up -d --build
	@echo "Waiting for parser/rag/executor/summarizer to report healthy..."
	@for i in $$(seq 1 40); do \
		unhealthy=$$(docker compose ps parser rag executor summarizer --format '{{.Health}}' 2>/dev/null | grep -vc '^healthy$$'); \
		if [ "$$unhealthy" = "0" ]; then break; fi; \
		sleep 2; \
	done
	docker compose exec parser python scripts/inject_incident.py --scenario connection_exhaustion
	@echo "Waiting for the summary to land in summarizer logs..."
	@sleep 5
	docker compose logs --tail=50 summarizer
	@echo ""
	@echo "Jaeger UI: http://localhost:16686"

chaos: up
	./scripts/chaos.sh

test:
	uv run pytest -m "not e2e"

logs:
	docker compose logs -f

lint:
	uv run ruff check .
	uv run ruff format --check .
