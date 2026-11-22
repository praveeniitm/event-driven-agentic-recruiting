"""Init container: create topics and register Avro schemas, idempotently.

Reads infra/topics.json (mounted at /infra) and schemas/*.avsc (mounted at /schemas).
Safe to re-run: existing topics get their configs re-applied, schemas that did not change
are not re-registered by Schema Registry (same schema -> same version).
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPICS_FILE = Path(os.getenv("TOPICS_FILE", "/infra/topics.json"))
SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", "/schemas"))
# PROD: BACKWARD_TRANSITIVE (or FULL_TRANSITIVE) so every old consumer can read every newer version.
COMPATIBILITY = os.getenv("SCHEMA_COMPATIBILITY", "BACKWARD")


def log(msg: str) -> None:
    print(f"[init] {msg}", flush=True)


def wait_for_kafka(admin: AdminClient, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=5)
            return
        except KafkaException:
            time.sleep(2)
    sys.exit("Kafka not reachable")


def ensure_topics(admin: AdminClient, spec: dict) -> None:
    existing = admin.list_topics(timeout=10).topics
    default_rf = spec["defaults"]["replication_factor"]

    to_create = [
        NewTopic(t["name"], num_partitions=t["partitions"], replication_factor=default_rf, config=t["config"])
        for t in spec["topics"]
        if t["name"] not in existing
    ]
    for name, fut in admin.create_topics(to_create).items() if to_create else []:
        fut.result()
        log(f"created topic {name}")

    # Re-apply configs on existing topics so edits in topics.json take effect.
    alter = [
        ConfigResource(ConfigResource.Type.TOPIC, t["name"], set_config=t["config"])
        for t in spec["topics"]
        if t["name"] in existing
    ]
    for res, fut in admin.alter_configs(alter).items() if alter else []:
        fut.result()
        log(f"configs applied to existing topic {res.name}")

    for t in spec["topics"]:
        if t["name"] in existing:
            actual = len(existing[t["name"]].partitions)
            if actual != t["partitions"]:
                # Kafka cannot reduce partitions, and increasing them re-maps keys.
                log(f"WARNING {t['name']}: has {actual} partitions, spec wants {t['partitions']}")


def registry_call(method: str, path: str, body: dict | None = None) -> dict:
    req = urllib.request.Request(
        f"{REGISTRY}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def wait_for_registry(timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            registry_call("GET", "/subjects")
            return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(2)
    sys.exit("Schema Registry not reachable")


def register_schemas(spec: dict) -> None:
    for t in spec["topics"]:
        if "value_schema" not in t:
            continue  # e.g. connect-dlq holds raw bytes
        subject = f"{t['name']}-value"  # TopicNameStrategy, the default in Confluent serializers
        schema_str = (SCHEMA_DIR / t["value_schema"]).read_text()
        json.loads(schema_str)  # fail fast on invalid JSON
        registry_call("PUT", f"/config/{subject}", {"compatibility": COMPATIBILITY})
        out = registry_call("POST", f"/subjects/{subject}/versions", {"schema": schema_str})
        log(f"schema {t['value_schema']} -> {subject} (id={out['id']}, compat={COMPATIBILITY})")


def main() -> None:
    spec = json.loads(TOPICS_FILE.read_text())
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    wait_for_kafka(admin)
    ensure_topics(admin, spec)
    wait_for_registry()
    register_schemas(spec)
    log("done")


if __name__ == "__main__":
    main()
