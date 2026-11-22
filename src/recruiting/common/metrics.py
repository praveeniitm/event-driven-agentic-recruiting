"""Prometheus metrics shared by all services. Each service exposes /metrics on its own port."""
from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

MESSAGES_CONSUMED = Counter("recruiting_messages_consumed_total", "Messages consumed", ["service", "topic"])
MESSAGES_PRODUCED = Counter("recruiting_messages_produced_total", "Messages produced", ["service", "topic"])
PRODUCE_ERRORS = Counter("recruiting_produce_errors_total", "Delivery failures", ["service", "topic"])
DLQ_MESSAGES = Counter("recruiting_dlq_messages_total", "Messages routed to a DLQ", ["service", "reason"])
DISCARDED = Counter("recruiting_discarded_total", "Messages dropped on purpose", ["service", "reason"])
LLM_CALLS = Counter("recruiting_llm_calls_total", "LLM invocations", ["service", "status"])
LLM_LATENCY = Histogram(
    "recruiting_llm_latency_seconds", "LLM call latency", ["service"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
PROCESSING_LATENCY = Histogram(
    "recruiting_processing_seconds", "End-to-end handling time per message", ["service"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30),
)
# PROD: add consumer-lag gauges (or run kafka-lag-exporter / Burrow) and alert when lag grows.


def start_metrics_server(port: int) -> None:
    start_http_server(port)
