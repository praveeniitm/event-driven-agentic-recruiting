"""End-to-end test against the running docker stack. Starts the real parser and matching services as subprocesses.

  docker compose up -d && uv run pytest -m integration
"""
import os
import subprocess
import sys
import time
import uuid

import psycopg
import pytest

from recruiting.common.config import get_settings
from recruiting.common.models import JobRequisition
from recruiting.services.producer.producer import EventProducer

pytestmark = pytest.mark.integration

RESUME = """Test Person
Email: test.person@example.com | Phone: +1 (415) 555-0199
Address: 1 Test Street, Testville

SUMMARY
Backend engineer with 6 years of experience building streaming systems in Python.
SKILLS
Python, Kafka, PostgreSQL, Docker"""


def _services_up():
    try:
        with psycopg.connect(get_settings().postgres_dsn, connect_timeout=2):
            return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def running_services():
    if not _services_up():
        pytest.skip("docker stack not running")
    env = {**os.environ, "LLM_PROVIDER": "mock"}
    # Distinct metrics ports are not needed in subprocesses because prometheus ports are bound per process: use a throwaway group.
    procs = [subprocess.Popen([sys.executable, "-m", f"recruiting.services.{s}"], env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for s in ("parser", "matching")]
    time.sleep(8)
    yield
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait(timeout=15)


def _wait(sql, params, timeout=60):
    deadline = time.time() + timeout
    with psycopg.connect(get_settings().postgres_dsn) as conn:
        while time.time() < deadline:
            rows = conn.execute(sql, params).fetchall()
            if rows:
                return rows
            time.sleep(1)
    return []


def test_resume_flows_through_pipeline_into_postgres(running_services):
    job_id = f"E2E-{uuid.uuid4().hex[:6]}"
    producer = EventProducer()
    producer.publish_job(JobRequisition(job_id=job_id, title="E2E Engineer", department="QA",
                                        required_skills=["Python", "Kafka"], min_experience_years=3))
    app = producer.submit_resume(job_id, "Test Person", "test.person@example.com", RESUME)
    rows = _wait("SELECT fit_score, recommendation, job_title FROM candidate_scores WHERE application_id=%s", (app.application_id,))
    assert rows, "score never reached Postgres via Kafka Connect"
    assert rows[0][1] == "STRONG_HIRE" and rows[0][2] == "E2E Engineer"


def test_corrupted_resume_reaches_dead_letters_table(running_services):
    job_id = f"E2E-{uuid.uuid4().hex[:6]}"
    app = EventProducer().submit_resume(job_id, "Bad Resume", "bad@example.com", "")
    rows = _wait("SELECT error_type, failed_service FROM dead_letters WHERE application_id=%s", (app.application_id,))
    assert rows and rows[0] == ("RESUME_TOO_SHORT", "parser")
