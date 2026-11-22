"""Pure aggregation logic for the bytewax job (kept free of Kafka so it is unit-testable)."""
from __future__ import annotations

from recruiting.common.models import JobStats, utc_now_iso

# state: application_id -> (fit_score, recommendation). Keeping the LATEST score per application makes the aggregate
# idempotent: a duplicate delivery (at-least-once) or a re-score overwrites instead of double counting.
JobState = dict[str, tuple[int, str]]


def update(state: JobState | None, score: dict) -> tuple[JobState, JobStats]:
    state = dict(state or {})
    state[score["application_id"]] = (score["fit_score"], score["recommendation"])
    values = list(state.values())
    count = len(values)
    return state, JobStats(
        job_id=score["job_id"],
        candidates_scored=count,
        avg_fit_score=round(sum(v[0] for v in values) / count, 2),
        strong_hire_count=sum(1 for v in values if v[1] == "STRONG_HIRE"),
        interview_count=sum(1 for v in values if v[1] == "INTERVIEW"),
        reject_count=sum(1 for v in values if v[1] == "REJECT"),
        updated_at=utc_now_iso(),
    )
