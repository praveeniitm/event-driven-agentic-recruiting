import pytest
from langchain_core.runnables import RunnableLambda

from recruiting.common.models import Recommendation
from recruiting.services.matching.evaluation import Evaluation, mock_evaluate, recommendation_for
from recruiting.services.matching.graph import build_matching_graph
from recruiting.services.matching.state_store import JobStateStore


@pytest.fixture
def job():
    return {"job_id": "JOB-1", "title": "Backend", "department": "Eng", "required_skills": ["Python", "Kafka"],
            "min_experience_years": 4, "status": "OPEN", "description": ""}


@pytest.fixture
def resume():
    return {"application_id": "a1", "job_id": "JOB-1", "anonymized_id": "anon", "extracted_skills": ["Python", "Kafka"],
            "total_experience_years": 6.0, "summary": "Backend engineer."}


def graph_for(job, fn=mock_evaluate, store=None, published=None):
    store = store if store is not None else JobStateStore()
    if job:
        store.put(job)
    published = published if published is not None else []
    g = build_matching_graph(RunnableLambda(fn), store, published.append, sleep=lambda _s: None, job_wait_seconds=0)
    return g, published


def test_scores_and_publishes(job, resume):
    g, published = graph_for(job)
    state = g.invoke({"resume": resume})
    assert state["outcome"] == "scored"
    assert published[0].fit_score == 100 and published[0].recommendation == Recommendation.STRONG_HIRE
    assert len(published[0].suggested_interview_questions) == 2 and published[0].job_title == "Backend"


def test_closed_job_is_discarded_and_nothing_is_published(job, resume):
    job["status"] = "CLOSED"
    g, published = graph_for(job)
    assert g.invoke({"resume": resume})["outcome"] == "discarded_closed"
    assert published == []


def test_unknown_job_is_failed(resume):
    g, published = graph_for(None)
    state = g.invoke({"resume": resume})
    assert state["outcome"] == "failed" and state["error"]["type"] == "JOB_NOT_FOUND" and published == []


def test_job_update_changes_decision(job, resume):
    store = JobStateStore()
    g, published = graph_for(job, store=store)
    g.invoke({"resume": resume})
    store.put({**job, "required_skills": ["Python", "Kafka", "Rust", "Go"], "min_experience_years": 10})  # newer job version wins
    g.invoke({"resume": resume})
    assert published[0].fit_score > published[1].fit_score


def test_llm_failing_twice_fails_with_trace(job, resume):
    def boom(_m):
        raise TimeoutError("down")
    g, published = graph_for(job, fn=boom)
    state = g.invoke({"resume": resume})
    assert state["outcome"] == "failed" and state["attempts"] == 2 and "Traceback" in state["error"]["trace"]
    assert published == []


def test_recommendation_guardrail_overrides_inconsistent_label(job, resume):
    def inconsistent(_m):
        return Evaluation(fit_score=20, recommendation=Recommendation.STRONG_HIRE, reasoning="x",
                          suggested_interview_questions=["q1", "q2"])
    g, published = graph_for(job, fn=inconsistent)
    g.invoke({"resume": resume})
    assert published[0].recommendation == Recommendation.REJECT


@pytest.mark.parametrize("score,rec", [(100, "STRONG_HIRE"), (85, "STRONG_HIRE"), (84, "INTERVIEW"), (45, "INTERVIEW"), (44, "REJECT")])
def test_thresholds(score, rec):
    assert recommendation_for(score).value == rec
