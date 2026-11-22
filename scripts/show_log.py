"""Pretty-print JSON service logs: python scripts/show_log.py /tmp/matching.log [field,field]"""
import json
import sys

fields = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
for line in open(sys.argv[1]):
    if not line.startswith("{"):
        continue
    d = json.loads(line)
    extra = {k: v for k, v in d.items() if k not in ("ts", "level", "service", "msg") and (not fields or k in fields)}
    print(f"{d['level']:<7} {d['msg']:<34} {extra}")
