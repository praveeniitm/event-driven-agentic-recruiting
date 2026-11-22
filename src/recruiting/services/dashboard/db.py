"""Read-only queries for the dashboard. The dashboard never writes to Postgres: Kafka Connect is the only writer."""
from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from recruiting.common.config import get_settings


def _query(sql: str, params: tuple = ()) -> list[dict]:
    # PROD: connection pool (psycopg_pool), a read-replica DSN, and a read-only role.
    with psycopg.connect(get_settings().postgres_dsn, row_factory=dict_row, connect_timeout=3) as conn:
        return conn.execute(sql, params).fetchall()


def fetch_scores(job_id: str | None = None, limit: int = 200) -> list[dict]:
    sql = """SELECT application_id, job_id, job_title, fit_score, recommendation, model, prompt_version, scored_at
             FROM candidate_scores {where} ORDER BY scored_at DESC LIMIT %s"""
    if job_id and job_id != "ALL":
        return _query(sql.format(where="WHERE job_id = %s"), (job_id, limit))
    return _query(sql.format(where=""), (limit,))


def fetch_score_detail(application_id: str) -> dict | None:
    rows = _query("SELECT * FROM candidate_scores WHERE application_id = %s", (application_id,))
    return rows[0] if rows else None


def fetch_dead_letters(limit: int = 50) -> list[dict]:
    return _query(
        """SELECT __connect_topic AS dlq, application_id, job_id, failed_service, error_type, error_message, attempts, failed_at
           FROM dead_letters ORDER BY failed_at DESC LIMIT %s""", (limit,))


def fetch_job_stats() -> list[dict]:
    return _query("""SELECT s.job_id, j.title, s.candidates_scored, s.avg_fit_score, s.strong_hire_count,
                            s.interview_count, s.reject_count
                     FROM job_stats s LEFT JOIN job_requisitions j USING (job_id) ORDER BY s.job_id""")


def fetch_jobs() -> list[dict]:
    return _query("SELECT * FROM job_requisitions ORDER BY job_id")
