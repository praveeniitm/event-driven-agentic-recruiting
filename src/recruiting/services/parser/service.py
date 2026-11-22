"""Parser agent service: candidate-applications -> parsed-resumes (+ pii-audit-log, + parser-dlq)."""
from __future__ import annotations

import base64
import json
import threading

from confluent_kafka import Message

from recruiting.common import metrics, topics
from recruiting.common.config import get_settings
from recruiting.common.kafka import AvroConsumer, AvroProducer
from recruiting.common.llm import build_structured_llm, model_name
from recruiting.common.logging import get_logger
from recruiting.common.models import (CandidateApplication, DeadLetter, ParsedResume, PiiAuditRecord,
                                      anonymize_email)
from recruiting.common.runner import run_consumer
from recruiting.services.parser.extraction import PROMPT_VERSION, ExtractedProfile, mock_extract
from recruiting.services.parser.graph import build_parser_graph

SERVICE = "parser"
GROUP_ID = "resume-parser-group"
METRICS_PORT = 8001


class ParserService:
    def __init__(self, producer: AvroProducer, graph=None) -> None:
        self.settings = get_settings()
        self.log = get_logger(SERVICE, self.settings.log_level)
        self.producer = producer
        self.graph = graph or build_parser_graph(build_structured_llm(ExtractedProfile, mock_extract))

    def handle(self, msg: Message, value: dict) -> None:
        app = CandidateApplication.model_validate(value)
        ctx = {"application_id": app.application_id, "job_id": app.job_id}
        state = self.graph.invoke({"application": app.model_dump()})

        if state.get("error"):
            err = state["error"]
            dlq = DeadLetter(
                application_id=app.application_id, job_id=app.job_id, failed_service=SERVICE,
                source_topic=msg.topic(), source_partition=msg.partition(), source_offset=msg.offset(),
                error_type=err["type"], error_message=err["message"], error_trace=err["trace"],
                attempts=state.get("attempts", 0), original_payload=app.model_dump_json(),
            )
            self.producer.produce(topics.PARSER_DLQ, key=app.application_id, value=dlq.model_dump(mode="json"))
            metrics.DLQ_MESSAGES.labels(SERVICE, err["type"]).inc()
            self.log.warning("routed to parser-dlq", extra={**ctx, "error_type": err["type"], "error": err["message"]})
            return

        anon = anonymize_email(app.candidate_email)
        red = state["redaction"]
        profile: ExtractedProfile = state["profile"]

        # Raw PII goes ONLY to the audit topic. Produced first: if it fails nothing clean is published.
        audit = PiiAuditRecord(
            application_id=app.application_id, anonymized_id=anon, redacted_fields=red.kinds,
            raw_pii={k: " | ".join(v) for k, v in red.found.items()}, redaction_count=red.count,
        )
        self.producer.produce(topics.PII_AUDIT_LOG, key=app.application_id, value=audit.model_dump(mode="json"))

        parsed = ParsedResume(
            application_id=app.application_id, job_id=app.job_id, anonymized_id=anon,
            extracted_skills=profile.extracted_skills, total_experience_years=profile.total_experience_years,
            summary=profile.summary, model=model_name(self.settings), prompt_version=PROMPT_VERSION,
        )
        self.producer.produce(topics.PARSED_RESUMES, key=app.job_id, value=parsed.model_dump(mode="json"),
                              headers={"correlation-id": app.application_id})
        self.log.info("parsed", extra={**ctx, "skills": len(parsed.extracted_skills),
                                       "years": parsed.total_experience_years, "pii_redactions": red.count})

    def handle_poison(self, msg: Message, exc: Exception) -> None:
        """Bytes that cannot be deserialized at all. Park them in the DLQ (base64) and move on."""
        raw = base64.b64encode(msg.value() or b"").decode()
        dlq = DeadLetter(
            application_id=f"{msg.topic()}-{msg.partition()}-{msg.offset()}", failed_service=SERVICE,
            source_topic=msg.topic(), source_partition=msg.partition(), source_offset=msg.offset(),
            error_type="DESERIALIZATION_ERROR", error_message=str(exc), error_trace="", attempts=1,
            original_payload=json.dumps({"base64": raw}),
        )
        self.producer.produce(topics.PARSER_DLQ, key=dlq.application_id, value=dlq.model_dump(mode="json"))
        metrics.DLQ_MESSAGES.labels(SERVICE, "DESERIALIZATION_ERROR").inc()


def main() -> None:
    settings = get_settings()
    metrics.start_metrics_server(METRICS_PORT)
    service = ParserService(AvroProducer(SERVICE, settings))
    consumer = AvroConsumer(SERVICE, GROUP_ID, [topics.CANDIDATE_APPLICATIONS], auto_offset_reset="earliest")
    run_consumer(SERVICE, consumer, service.handle, service.handle_poison, service.log, threading.Event())
