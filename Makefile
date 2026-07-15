.PHONY: build up down demo demo-alt chaos demo-request demo-request-invalid test logs lint

build:
	docker compose build

up:
	docker compose up -d

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
	SYSTEM_CONFIG_FILE=triage_with_summary.yaml docker compose --profile with-summary up -d
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

demo-request:
	docker compose --profile maintenance up -d
	@echo "Waiting for request-parser/compliance-rag/maintenance-planner to report healthy..."
	@for i in $$(seq 1 40); do \
		unhealthy=$$(docker compose ps request-parser compliance-rag maintenance-planner --format '{{.Health}}' 2>/dev/null | grep -vc '^healthy$$'); \
		if [ "$$unhealthy" = "0" ]; then break; fi; \
		sleep 2; \
	done
	docker compose exec request-parser python scripts/inject_request.py --scenario os_update
	@echo "Waiting for the plan to land in maintenance-planner logs..."
	@sleep 5
	docker compose logs --tail=50 maintenance-planner
	@echo ""
	@echo "Jaeger UI (trace across request-parser -> compliance-rag -> maintenance-planner): http://localhost:16686"

demo-request-invalid:
	docker compose --profile maintenance up -d
	@echo "Waiting for request-parser/compliance-rag/maintenance-planner to report healthy..."
	@for i in $$(seq 1 40); do \
		unhealthy=$$(docker compose ps request-parser compliance-rag maintenance-planner --format '{{.Health}}' 2>/dev/null | grep -vc '^healthy$$'); \
		if [ "$$unhealthy" = "0" ]; then break; fi; \
		sleep 2; \
	done
	@inject_output=$$(docker compose exec -T request-parser python scripts/inject_request.py --scenario invalid_object_type); \
	echo "$$inject_output"; \
	correlation_id=$$(echo "$$inject_output" \
	  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
	  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/'); \
	echo "Waiting up to 60s for max_deliver to exhaust and correlation_id=$$correlation_id to land in dlq.parse_request..."; \
	docker compose exec -T request-parser python scripts/show_dlq.py --subject dlq.parse_request --correlation-id "$$correlation_id" --wait-seconds 60

test:
	uv run pytest -m "not e2e"

logs:
	docker compose logs -f

lint:
	uv run ruff check .
	uv run ruff format --check .
