.PHONY: first-run build up down demo demo-alt chaos demo-request demo-request-invalid test logs lint

first-run:
	$(MAKE) build
	$(MAKE) demo

build:
	docker compose --profile build build agent-image

up:
	docker compose up -d

down:
	docker compose --profile build --profile with-summary --profile maintenance down -v --remove-orphans

demo: up
	@echo "Waiting for parser/rag/executor to report healthy..."
	@ready=0; \
	for i in $$(seq 1 40); do \
		healthy=$$(docker compose ps parser rag executor --format '{{.Health}}' 2>/dev/null | grep -c '^healthy$$' || true); \
		if [ "$$healthy" = "3" ]; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" != "1" ]; then \
		echo "ERROR: parser/rag/executor did not all become healthy" >&2; \
		docker compose ps parser rag executor >&2; \
		exit 1; \
	fi
	@inject_output=$$(docker compose exec -T parser python scripts/inject_incident.py --scenario connection_exhaustion); \
	echo "$$inject_output"; \
	correlation_id=$$(echo "$$inject_output" \
	  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
	  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/'); \
	if [ -z "$$correlation_id" ]; then \
		echo "ERROR: could not extract correlation_id from inject_incident.py output" >&2; \
		exit 1; \
	fi; \
	echo "Waiting up to 60s for events.incident.completed from executor (correlation_id=$$correlation_id)..."; \
	if ! docker compose exec -T parser python scripts/wait_for_message.py \
	  --subject events.incident.completed --correlation-id "$$correlation_id" \
	  --sender executor --type event --wait-seconds 60; then \
		docker compose logs --tail=100 executor >&2; \
		exit 1; \
	fi; \
	docker compose logs --tail=50 executor
	@echo ""
	@echo "Jaeger UI (trace across parser -> rag -> executor): http://localhost:16686"

demo-alt:
	SYSTEM_CONFIG_FILE=triage_with_summary.yaml docker compose --profile with-summary up -d
	@echo "Waiting for parser/rag/executor/summarizer to report healthy..."
	@ready=0; \
	for i in $$(seq 1 40); do \
		healthy=$$(docker compose ps parser rag executor summarizer --format '{{.Health}}' 2>/dev/null | grep -c '^healthy$$' || true); \
		if [ "$$healthy" = "4" ]; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" != "1" ]; then \
		echo "ERROR: parser/rag/executor/summarizer did not all become healthy" >&2; \
		docker compose ps parser rag executor summarizer >&2; \
		exit 1; \
	fi
	@inject_output=$$(docker compose exec -T parser python scripts/inject_incident.py --scenario connection_exhaustion); \
	echo "$$inject_output"; \
	correlation_id=$$(echo "$$inject_output" \
	  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
	  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/'); \
	if [ -z "$$correlation_id" ]; then \
		echo "ERROR: could not extract correlation_id from inject_incident.py output" >&2; \
		exit 1; \
	fi; \
	echo "Waiting up to 60s for events.incident.completed from summarizer (correlation_id=$$correlation_id)..."; \
	if ! docker compose exec -T parser python scripts/wait_for_message.py \
	  --subject events.incident.completed --correlation-id "$$correlation_id" \
	  --sender summarizer --type event --wait-seconds 60; then \
		docker compose logs --tail=100 summarizer >&2; \
		exit 1; \
	fi; \
	docker compose logs --tail=50 summarizer
	@echo ""
	@echo "Jaeger UI: http://localhost:16686"

chaos:
	SYSTEM_CONFIG_FILE=incident_triage_chaos.yaml docker compose up -d
	./scripts/chaos.sh

demo-request:
	docker compose --profile maintenance up -d
	@echo "Waiting for request-parser/compliance-rag/maintenance-planner to report healthy..."
	@ready=0; \
	for i in $$(seq 1 40); do \
		healthy=$$(docker compose ps request-parser compliance-rag maintenance-planner --format '{{.Health}}' 2>/dev/null | grep -c '^healthy$$' || true); \
		if [ "$$healthy" = "3" ]; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" != "1" ]; then \
		echo "ERROR: request-parser/compliance-rag/maintenance-planner did not all become healthy" >&2; \
		docker compose ps request-parser compliance-rag maintenance-planner >&2; \
		exit 1; \
	fi
	@inject_output=$$(docker compose exec -T request-parser python scripts/inject_request.py --scenario os_update); \
	echo "$$inject_output"; \
	correlation_id=$$(echo "$$inject_output" \
	  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
	  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/'); \
	if [ -z "$$correlation_id" ]; then \
		echo "ERROR: could not extract correlation_id from inject_request.py output" >&2; \
		exit 1; \
	fi; \
	echo "Waiting up to 60s for events.maintenance.completed from maintenance-planner (correlation_id=$$correlation_id)..."; \
	if ! docker compose exec -T request-parser python scripts/wait_for_message.py \
	  --subject events.maintenance.completed --correlation-id "$$correlation_id" \
	  --sender maintenance-planner --type event --wait-seconds 60; then \
		docker compose logs --tail=100 maintenance-planner >&2; \
		exit 1; \
	fi; \
	docker compose logs --tail=50 maintenance-planner
	@echo ""
	@echo "Jaeger UI (trace across request-parser -> compliance-rag -> maintenance-planner): http://localhost:16686"

demo-request-invalid:
	docker compose --profile maintenance up -d
	@echo "Waiting for request-parser/compliance-rag/maintenance-planner to report healthy..."
	@ready=0; \
	for i in $$(seq 1 40); do \
		healthy=$$(docker compose ps request-parser compliance-rag maintenance-planner --format '{{.Health}}' 2>/dev/null | grep -c '^healthy$$' || true); \
		if [ "$$healthy" = "3" ]; then ready=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ready" != "1" ]; then \
		echo "ERROR: request-parser/compliance-rag/maintenance-planner did not all become healthy" >&2; \
		docker compose ps request-parser compliance-rag maintenance-planner >&2; \
		exit 1; \
	fi
	@inject_output=$$(docker compose exec -T request-parser python scripts/inject_request.py --scenario invalid_object_type); \
	echo "$$inject_output"; \
	correlation_id=$$(echo "$$inject_output" \
	  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
	  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/'); \
	if [ -z "$$correlation_id" ]; then \
		echo "ERROR: could not extract correlation_id from inject_request.py output" >&2; \
		exit 1; \
	fi; \
	echo "Waiting up to 60s for max_deliver to exhaust and correlation_id=$$correlation_id to land in dlq.parse_request..."; \
	docker compose exec -T request-parser python scripts/show_dlq.py --subject dlq.parse_request --correlation-id "$$correlation_id" --wait-seconds 60

test:
	uv run pytest -m "not e2e"

logs:
	docker compose logs -f

lint:
	uv run ruff check .
	uv run ruff format --check .
