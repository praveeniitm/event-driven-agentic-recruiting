"""Compare candidate-scores on Kafka with the seed labels (quick manual check; eval/ has the real harness)."""
import uuid

from recruiting.common.kafka import AvroConsumer
from recruiting.services.producer.seed_data import RESUMES, build_applications

expected = {a.application_id: r["expected"] for a, r in zip(build_applications(), RESUMES, strict=True)}
c = AvroConsumer("compare", f"compare-{uuid.uuid4()}", ["candidate-scores"])
idle, rows = 0, []
while idle < 4:
    m = c.poll(1.0)
    if m is None:
        idle += 1
        continue
    v = c.decode(m)
    rows.append((v["job_title"], v["fit_score"], v["recommendation"], expected.get(v["application_id"], "?")))
c.close()
for title, score, rec, exp in sorted(rows):
    print(f"{title:<26} {score:>3} {rec:<11} expected={exp:<11} {'OK' if exp == rec else 'DIFF'}")
print(f"-- {len(rows)} scores, {sum(1 for r in rows if r[2] == r[3])} match the seed labels")
