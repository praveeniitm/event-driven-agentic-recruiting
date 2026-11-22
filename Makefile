.PHONY: help up down reset install seed parser matching analytics dashboard test integration eval lint inspect lag apps observability

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

up:              ## start Kafka, Schema Registry, Connect, Kafka UI, Postgres (+ init containers)
	docker compose up -d --build
down:            ## stop everything, keep data
	docker compose --profile apps --profile observability down
reset:           ## stop everything and DELETE all data volumes
	docker compose --profile apps --profile observability down -v
install:         ## create .venv with Python 3.11 and install deps
	uv sync
seed:            ## publish 3 jobs and 10 resumes (one corrupted -> DLQ)
	uv run python -m recruiting.services.producer seed --demo-compaction
parser:          ## run the parser agent on the host
	uv run python -m recruiting.services.parser
matching:        ## run the matching agent on the host
	uv run python -m recruiting.services.matching
analytics:       ## run the bytewax job on the host
	uv run python -m recruiting.services.analytics
dashboard:       ## Gradio UI on http://localhost:7860
	uv run python app.py
apps:            ## run parser, matching, analytics and dashboard as containers
	docker compose --profile apps up -d --build
observability:   ## Prometheus :9091 and Grafana :3000
	docker compose --profile observability up -d
test:            ## unit + contract tests (no Docker needed)
	uv run pytest
integration:     ## end-to-end tests against the running stack
	uv run pytest -m integration
eval:            ## offline quality evaluation of the agents (add JUDGE=1 for LLM-as-judge)
	uv run python -m recruiting.eval.run $(if $(JUDGE),--judge,)
lint:
	uv run ruff check src tests scripts app.py
inspect:         ## usage: make inspect TOPIC=parsed-resumes
	uv run python scripts/inspect_topic.py $(TOPIC)
lag:             ## consumer group lag
	docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --all-groups
