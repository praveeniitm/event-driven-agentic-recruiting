"""Scan a topic's values for any seed-candidate PII (name parts, email, phone). Exit 1 if anything leaks.

  uv run python scripts/scan_pii_leaks.py parsed-resumes
"""
from __future__ import annotations

import json
import re
import sys
import uuid

from recruiting.common.kafka import AvroConsumer
from recruiting.services.producer.seed_data import RESUMES


def main() -> int:
    topic = sys.argv[1] if len(sys.argv) > 1 else "parsed-resumes"
    consumer = AvroConsumer("pii-scan", f"pii-scan-{uuid.uuid4()}", [topic])
    blobs, idle = [], 0
    while idle < 4:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        blobs.append(json.dumps(consumer.decode(msg)).lower())
    consumer.close()
    text = " ".join(blobs)
    def present(token: str) -> bool:
        return re.search(rf"(?<![\w@]){re.escape(token.lower())}(?![\w@])", text) is not None

    # Whole-word match (so "Tom" does not match "automation"). The corrupted resume is never parsed, so it is skipped.
    leaks = [(r["name"], t) for r in RESUMES if r["text"].strip()
             for t in [r["email"], r["phone"], *r["name"].split()] if t and present(t)]
    print(f"scanned {len(blobs)} message(s) on {topic}; leaks: {leaks or 'none'}")
    return 1 if leaks else 0


if __name__ == "__main__":
    sys.exit(main())
