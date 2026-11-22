"""In-memory materialised view ("table") of the compacted job-requisitions topic.

This is the "table" side of a stream-table join: parsed-resumes is the stream, this dict is the table.
  * Reads the topic from offset 0 with assign() (no consumer group offsets, so every restart rebuilds the full table).
  * Keeps following the topic so job edits and CLOSED status apply while the agent runs.
  * wait_until_ready() blocks until the initial backlog is read, so the join never runs against a half-loaded table.
PROD: use RocksDB (Kafka Streams / Faust / bytewax recovery) for tables that do not fit in memory, and run
one loader per partition-assignment so the table is co-partitioned with the stream.
"""
from __future__ import annotations

import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from recruiting.common import topics
from recruiting.common.config import Settings, get_settings
from recruiting.common.kafka import schema_registry_client


class JobStateStore:
    def __init__(self, settings: Settings | None = None, log: logging.Logger | None = None) -> None:
        self.settings = settings or get_settings()
        self.log = log or logging.getLogger("job-state-store")
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="job-state-loader", daemon=True)

    # ---- table API ----
    def get(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job else None

    def wait_for(self, job_id: str, timeout_s: float) -> dict | None:
        """Wait briefly for a job that may not have arrived yet (stream and table are different topics, so order is not guaranteed)."""
        deadline = time.monotonic() + timeout_s
        while True:
            job = self.get(job_id)
            if job is not None or time.monotonic() >= deadline:
                return job
            time.sleep(0.25)

    def __len__(self) -> int:
        with self._lock:
            return len(self._jobs)

    def put(self, job: dict) -> None:  # also used by tests
        with self._lock:
            self._jobs[job["job_id"]] = job

    # ---- lifecycle ----
    def start(self, ready_timeout_s: float = 60) -> None:
        self._thread.start()
        if not self._ready.wait(ready_timeout_s):
            raise TimeoutError("job state store did not catch up in time")
        self.log.info("job state store ready", extra={"jobs": len(self)})

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        consumer = Consumer({
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "group.id": "matching-agent-job-loader",  # required by the client, never used for commits
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        })
        deserializer = AvroDeserializer(schema_registry_client(self.settings))
        meta = consumer.list_topics(topics.JOB_REQUISITIONS, timeout=15)
        partitions = list(meta.topics[topics.JOB_REQUISITIONS].partitions)
        consumer.assign([TopicPartition(topics.JOB_REQUISITIONS, p, OFFSET_BEGINNING) for p in partitions])
        caught_up: set[int] = set()
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        caught_up.add(msg.partition())
                        if len(caught_up) == len(partitions):
                            self._ready.set()
                    else:
                        self.log.error("job loader error", extra={"error": str(err)})
                    continue
                try:
                    job = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                except Exception as exc:  # a bad job record must not kill the loader
                    self.log.error("skipping undecodable job record", extra={"error": str(exc), "offset": msg.offset()})
                    continue
                if msg.value() is None:  # tombstone
                    continue
                self.put(job)
                if job["status"] == "CLOSED":
                    self.log.info("job closed", extra={"job_id": job["job_id"]})
        finally:
            consumer.close()
