"""Thin Avro-aware wrappers around confluent-kafka producers and consumers."""
from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from recruiting.common import metrics
from recruiting.common.config import Settings, get_settings
from recruiting.common.topics import VALUE_SCHEMA_FILES


class DeliveryError(RuntimeError):
    pass


def schema_registry_client(settings: Settings | None = None) -> SchemaRegistryClient:
    s = settings or get_settings()
    return SchemaRegistryClient({"url": s.schema_registry_url})


class AvroProducer:
    """Idempotent Avro producer. Keys are plain UTF-8 strings, values are Avro via Schema Registry.

    Schemas are registered by the init container, so the producer never auto-registers
    (PROD: producers must not be able to change contracts, only CI/CD should).
    """

    def __init__(self, service: str, settings: Settings | None = None) -> None:
        self.service = service
        self.settings = settings or get_settings()
        self._producer = Producer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "client.id": service,
                "enable.idempotence": True,  # no duplicates / reordering from producer retries
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 5,
                # PROD: use transactional.id + send_offsets_to_transaction for exactly-once consume-process-produce.
            }
        )
        self._sr = schema_registry_client(self.settings)
        self._key = StringSerializer("utf_8")
        self._values: dict[str, AvroSerializer] = {}

    def _value_serializer(self, topic: str) -> AvroSerializer:
        if topic not in self._values:
            schema_str = (self.settings.schema_dir / VALUE_SCHEMA_FILES[topic]).read_text()
            self._values[topic] = AvroSerializer(
                self._sr, schema_str, conf={"auto.register.schemas": False, "use.latest.version": True}
            )
        return self._values[topic]

    def produce(
        self,
        topic: str,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
        wait: bool = True,
        timeout: float = 15.0,
    ) -> None:
        """Serialize and send. With wait=True this blocks until the broker acks (needed before committing offsets)."""
        errors: list[str] = []

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                errors.append(str(err))
                metrics.PRODUCE_ERRORS.labels(self.service, topic).inc()
            else:
                metrics.MESSAGES_PRODUCED.labels(self.service, topic).inc()

        payload = self._value_serializer(topic)(value, SerializationContext(topic, MessageField.VALUE))
        self._producer.produce(
            topic=topic,
            key=self._key(key),
            value=payload,
            headers=list((headers or {}).items()),
            on_delivery=on_delivery,
        )
        if wait:
            remaining = self._producer.flush(timeout)
            if remaining:
                raise DeliveryError(f"{remaining} message(s) not delivered to {topic} within {timeout}s")
            if errors:
                raise DeliveryError(f"delivery to {topic} failed: {errors[0]}")
        else:
            self._producer.poll(0)

    def flush(self, timeout: float = 15.0) -> None:
        self._producer.flush(timeout)


class AvroConsumer:
    """Consumer with manual commits and an Avro value deserializer."""

    def __init__(
        self,
        service: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
        settings: Settings | None = None,
        extra_conf: dict[str, Any] | None = None,
    ) -> None:
        self.service = service
        self.settings = settings or get_settings()
        conf = {
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "client.id": service,
            "enable.auto.commit": False,  # offsets are committed only after the result is safely produced
            "auto.offset.reset": auto_offset_reset,
            "isolation.level": "read_committed",
            "max.poll.interval.ms": 300000,  # LLM calls are slow, so allow plenty of time between polls
            # PROD: partition.assignment.strategy=cooperative-sticky and static group.instance.id to avoid rebalance storms.
        }
        conf.update(extra_conf or {})
        self._consumer = Consumer(conf)
        self._deserializer = AvroDeserializer(schema_registry_client(self.settings))
        self._consumer.subscribe(topics)

    def poll(self, timeout: float = 1.0) -> Message | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(f"consumer error: {err}")
        metrics.MESSAGES_CONSUMED.labels(self.service, msg.topic()).inc()
        return msg

    def decode(self, msg: Message) -> dict[str, Any]:
        """Raises on a poison pill (bad bytes / unknown schema). Callers must route that to a DLQ."""
        return self._deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

    def commit(self, msg: Message) -> None:
        self._consumer.commit(message=msg, asynchronous=False)

    def rewind(self, msg: Message) -> None:
        """Seek back to this message so it is re-delivered by the next poll (used when an output could not be produced)."""
        self._consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))

    def close(self) -> None:
        self._consumer.close()


def log_kafka_versions(logger: logging.Logger) -> None:
    import confluent_kafka

    logger.info("confluent-kafka %s librdkafka %s", confluent_kafka.version()[0], confluent_kafka.libversion()[0])
