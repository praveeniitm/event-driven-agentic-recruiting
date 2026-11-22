"""bytewax dataflow: candidate-scores -> per-job rolling stats -> job-stats (compacted topic).

Pure Python stateful stream processing, no JVM. Run with either:
    uv run python -m recruiting.services.analytics
    uv run python -m bytewax.run recruiting.services.analytics.flow:flow      (supports -w workers)

Notes
  * Parallelism: state is keyed by job_id, bytewax routes the same key to the same worker.
  * PROD: enable bytewax recovery (`-r recovery_dir -s 10`, or Postgres/SQLite backed) so state survives restarts
    without replaying the whole topic, and use windowed aggregation if you need "last 24h" style metrics.
"""
from __future__ import annotations

import logging

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from recruiting.common import metrics, topics
from recruiting.common.config import get_settings
from recruiting.common.kafka import schema_registry_client
from recruiting.services.analytics.aggregate import update

log = logging.getLogger("analytics")


def build_flow() -> Dataflow:
    settings = get_settings()
    sr = schema_registry_client(settings)
    deserializer = AvroDeserializer(sr)
    serializer = AvroSerializer(
        sr, (settings.schema_dir / topics.VALUE_SCHEMA_FILES[topics.JOB_STATS]).read_text(),
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )

    flow = Dataflow("job-stats")
    msgs = kop.input("kafka-in", flow, brokers=[settings.kafka_bootstrap_servers], topics=[topics.CANDIDATE_SCORES],
                     starting_offset=-2)  # -2 = OFFSET_BEGINNING: rebuild state from the whole topic on start
    op.inspect("kafka-errors", msgs.errs)

    def decode(m):
        try:
            metrics.MESSAGES_CONSUMED.labels("analytics", topics.CANDIDATE_SCORES).inc()
            return deserializer(m.value, SerializationContext(topics.CANDIDATE_SCORES, MessageField.VALUE))
        except Exception:
            log.exception("dropping undecodable score")  # PROD: send to a DLQ instead of dropping.
            return None

    scores = op.filter_map("decode", msgs.oks, decode)
    keyed = op.key_on("by-job", scores, lambda s: s["job_id"])
    stats = op.stateful_map("aggregate", keyed, lambda state, score: update(state, score))

    def encode(item):
        job_id, js = item
        value = serializer(js.model_dump(mode="json"), SerializationContext(topics.JOB_STATS, MessageField.VALUE))
        metrics.MESSAGES_PRODUCED.labels("analytics", topics.JOB_STATS).inc()
        return KafkaSinkMessage(key=job_id.encode(), value=value)

    kop.output("kafka-out", op.map("encode", stats, encode), brokers=[settings.kafka_bootstrap_servers],
               topic=topics.JOB_STATS)
    return flow


flow = build_flow()
