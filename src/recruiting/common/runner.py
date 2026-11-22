"""Generic consume loop shared by the agents: poll -> decode -> handle -> commit.

Delivery guarantee is at-least-once: a crash between produce and commit re-delivers the message, so
downstream writes are idempotent (the Postgres sink upserts on application_id).
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from typing import Callable

from confluent_kafka import Message

from recruiting.common import metrics
from recruiting.common.kafka import AvroConsumer, DeliveryError

Handler = Callable[[Message, dict], None]
PoisonHandler = Callable[[Message, Exception], None]


def install_signal_handlers(stop: threading.Event) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())


def run_consumer(
    service: str,
    consumer: AvroConsumer,
    handle: Handler,
    on_poison: PoisonHandler,
    log: logging.Logger,
    stop: threading.Event | None = None,
    idle_hook: Callable[[], None] | None = None,
) -> None:
    stop = stop or threading.Event()
    install_signal_handlers(stop) if threading.current_thread() is threading.main_thread() else None
    log.info("consumer started")
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                if idle_hook:
                    idle_hook()
                continue
            start = time.perf_counter()
            try:
                try:
                    value = consumer.decode(msg)
                except Exception as exc:  # poison pill: bytes we can never decode
                    log.error("undecodable message", extra={"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()})
                    on_poison(msg, exc)
                    consumer.commit(msg)
                    continue
                handle(msg, value)
                consumer.commit(msg)  # only after outputs (result or DLQ) are acked by the broker
            except DeliveryError as exc:
                # Output could not be produced: do NOT commit. Rewind so the same message is retried after a pause.
                log.error("produce failed, will retry", extra={"error": str(exc), "offset": msg.offset()})
                consumer.rewind(msg)
                time.sleep(2)  # PROD: exponential backoff with jitter and a max-retry alert.
            finally:
                metrics.PROCESSING_LATENCY.labels(service).observe(time.perf_counter() - start)
    finally:
        log.info("consumer closing")
        consumer.close()
