"""Dump a topic from the beginning with partition/offset/key and the Avro-decoded value.

  uv run python scripts/inspect_topic.py candidate-applications [--limit 20] [--fields application_id,job_id]
"""
from __future__ import annotations

import argparse
import json
import uuid

from recruiting.common.kafka import AvroConsumer


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("topic")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--fields", help="comma separated subset of value fields to print")
    p.add_argument("--idle-seconds", type=int, default=5)
    args = p.parse_args()

    # Random throwaway group, never commits, so inspecting does not disturb real consumer groups.
    consumer = AvroConsumer("inspector", f"inspector-{uuid.uuid4()}", [args.topic], auto_offset_reset="earliest")
    wanted = args.fields.split(",") if args.fields else None
    seen, idle = 0, 0
    try:
        while seen < args.limit and idle < args.idle_seconds:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            idle = 0
            value = consumer.decode(msg)
            if wanted:
                value = {k: value[k] for k in wanted}
            key = msg.key().decode() if msg.key() else None
            print(f"p{msg.partition()} @{msg.offset():<3} key={key} {json.dumps(value)[:300]}")
            seen += 1
    finally:
        consumer.close()
    print(f"-- {seen} message(s) from {args.topic}")


if __name__ == "__main__":
    main()
