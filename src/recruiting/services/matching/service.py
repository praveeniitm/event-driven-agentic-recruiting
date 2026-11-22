"""Matching agent service: parsed-resumes JOIN job-requisitions table -> candidate-scores."""
from __future__ import annotations

import base64
import json
import threading

from confluent_kafka import Message

from recruiting.common import metrics, topics
from recruiting.common.config import get_settings
from recruiting.common.kafka import AvroConsumer, AvroProducer
from recruiting.common.llm import build_structured_llm
from recruiting.common.logging import get_logger
from recruiting.common.models import CandidateScore, DeadLetter, ParsedResume
from recruiting.common.runner import run_consumer
from recruiting.services.matching.evaluation import Evaluation, mock_evaluate
from recruiting.services.matching.graph import build_matching_graph
from recruiting.services.matching.state_store import JobStateStore

SERVICE = "matching"
GROUP_ID = "matching-agent-group"
METRICS_PORT = 8002


class MatchingService:
    def __init__(self, producer: AvroProducer, jobs: JobStateStore, llm=None, graph_kwargs: dict | None = None) -> None:
        self.settings = get_settings()
        self.log = get_logger(SERVICE, self.settings.log_level)
        self.producer = producer
        self.graph = build_matching_graph(
            llm or build_structured_llm(Evaluation, mock_evaluate), jobs, self._publish, **(graph_kwargs or {})
        )

    def _publish(self, score: CandidateScore) -> None:
        self.producer.produce(topics.CANDIDATE_SCORES, key=score.job_id, value=score.model_dump(mode="json"),
                              headers={"correlation-id": score.application_id})

    def handle(self, msg: Message, value: dict) -> None:
        resume = ParsedResume.model_validate(value)
        ctx = {"application_id": resume.application_id, "job_id": resume.job_id}
        state = self.graph.invoke({"resume": resume.model_dump()})
        outcome = state.get("outcome")

        if outcome == "scored":
            s = state["score"]
            self.log.info("scored", extra={**ctx, "fit_score": s.fit_score, "recommendation": s.recommendation.value})
        elif outcome == "discarded_closed":
            metrics.DISCARDED.labels(SERVICE, "job_closed").inc()
            self.log.info("discarded: job is closed", extra=ctx)
        else:
            err = state["error"]
            dlq = DeadLetter(
                application_id=resume.application_id, job_id=resume.job_id, failed_service=SERVICE,
                source_topic=msg.topic(), source_partition=msg.partition(), source_offset=msg.offset(),
                error_type=err["type"], error_message=err["message"], error_trace=err["trace"],
                attempts=state.get("attempts", 0), original_payload=resume.model_dump_json(),
            )
            self.producer.produce(topics.MATCHING_DLQ, key=resume.application_id, value=dlq.model_dump(mode="json"))
            metrics.DLQ_MESSAGES.labels(SERVICE, err["type"]).inc()
            self.log.warning("routed to matching-dlq", extra={**ctx, "error_type": err["type"], "error": err["message"]})

    def handle_poison(self, msg: Message, exc: Exception) -> None:
        dlq = DeadLetter(
            application_id=f"{msg.topic()}-{msg.partition()}-{msg.offset()}", failed_service=SERVICE,
            source_topic=msg.topic(), source_partition=msg.partition(), source_offset=msg.offset(),
            error_type="DESERIALIZATION_ERROR", error_message=str(exc), error_trace="", attempts=1,
            original_payload=json.dumps({"base64": base64.b64encode(msg.value() or b"").decode()}),
        )
        self.producer.produce(topics.MATCHING_DLQ, key=dlq.application_id, value=dlq.model_dump(mode="json"))
        metrics.DLQ_MESSAGES.labels(SERVICE, "DESERIALIZATION_ERROR").inc()


def main() -> None:
    settings = get_settings()
    metrics.start_metrics_server(METRICS_PORT)
    jobs = JobStateStore(settings, get_logger("job-state-store", settings.log_level))
    jobs.start()  # blocks until the compacted topic is fully loaded, BEFORE any resume is processed
    service = MatchingService(AvroProducer(SERVICE, settings), jobs)
    consumer = AvroConsumer(SERVICE, GROUP_ID, [topics.PARSED_RESUMES], auto_offset_reset="earliest")
    try:
        run_consumer(SERVICE, consumer, service.handle, service.handle_poison, service.log, threading.Event())
    finally:
        jobs.stop()
