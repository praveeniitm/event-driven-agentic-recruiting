"""LangGraph workflow for the matching agent.

START -> fetch_job_context --(job closed / missing / error)--> END
                |
                v
         evaluate_candidate --(error)--> END
                |
                v
         publish_decision -> END

Business outcomes are written to state["outcome"]: scored | discarded_closed | failed.
Kafka delivery errors are NOT swallowed here. They propagate so the consumer loop does not commit.
"""
from __future__ import annotations

import time
import traceback
from typing import Callable, TypedDict

from langchain_core.runnables import Runnable
from langgraph.graph import END, START, StateGraph

from recruiting.common.llm import call_llm, model_name
from recruiting.common.models import CandidateScore, utc_now_iso
from recruiting.services.matching.evaluation import PROMPT_VERSION, Evaluation, build_messages, recommendation_for
from recruiting.services.matching.state_store import JobStateStore

MAX_LLM_ATTEMPTS = 2
JOB_WAIT_SECONDS = 5.0  # PROD: tune to producer->consumer skew, or park the record in a retry topic instead of blocking.


class MatchState(TypedDict, total=False):
    resume: dict
    job: dict
    evaluation: Evaluation
    score: CandidateScore
    outcome: str
    attempts: int
    error: dict | None


def build_matching_graph(
    llm: Runnable,
    jobs: JobStateStore,
    publish: Callable[[CandidateScore], None],
    sleep: Callable[[float], None] = time.sleep,
    job_wait_seconds: float = JOB_WAIT_SECONDS,
):
    def fetch_job_context(state: MatchState) -> MatchState:
        resume = state["resume"]
        job = jobs.wait_for(resume["job_id"], job_wait_seconds)
        if job is None:
            return {"outcome": "failed", "error": {"type": "JOB_NOT_FOUND", "trace": "",
                                                    "message": f"no job requisition for {resume['job_id']}"}}
        if job["status"] == "CLOSED":
            return {"job": job, "outcome": "discarded_closed"}
        return {"job": job}

    def route_after_fetch(state: MatchState) -> str:
        return "evaluate" if "outcome" not in state else "stop"

    def evaluate_candidate(state: MatchState) -> MatchState:
        messages = build_messages(state["job"], state["resume"])
        last_exc: Exception | None = None
        last_trace = ""
        for attempt in range(1, MAX_LLM_ATTEMPTS + 1):
            try:
                ev = call_llm(llm, messages, "matching")
                assert isinstance(ev, Evaluation)
                # Guardrail: the label must follow the score thresholds. Code owns policy, the model only proposes.
                expected = recommendation_for(ev.fit_score)
                if ev.recommendation != expected:
                    ev = ev.model_copy(update={"recommendation": expected})
                return {"evaluation": ev, "attempts": attempt}
            except Exception as exc:
                last_exc, last_trace = exc, traceback.format_exc()
                if attempt < MAX_LLM_ATTEMPTS:
                    sleep(0.5 * attempt)
        return {"attempts": MAX_LLM_ATTEMPTS, "outcome": "failed",
                "error": {"type": "LLM_FAILURE", "message": str(last_exc), "trace": last_trace}}

    def route_after_eval(state: MatchState) -> str:
        return "stop" if state.get("outcome") == "failed" else "publish"

    def publish_decision(state: MatchState) -> MatchState:
        r, job, ev = state["resume"], state["job"], state["evaluation"]
        score = CandidateScore(
            application_id=r["application_id"], job_id=r["job_id"], anonymized_id=r["anonymized_id"],
            fit_score=ev.fit_score, recommendation=ev.recommendation, reasoning=ev.reasoning,
            suggested_interview_questions=ev.suggested_interview_questions, job_title=job["title"],
            model=model_name(), prompt_version=PROMPT_VERSION, scored_at=utc_now_iso(),
        )
        publish(score)
        return {"score": score, "outcome": "scored"}

    g = StateGraph(MatchState)
    g.add_node("fetch_job_context", fetch_job_context)
    g.add_node("evaluate_candidate", evaluate_candidate)
    g.add_node("publish_decision", publish_decision)
    g.add_edge(START, "fetch_job_context")
    g.add_conditional_edges("fetch_job_context", route_after_fetch, {"evaluate": "evaluate_candidate", "stop": END})
    g.add_conditional_edges("evaluate_candidate", route_after_eval, {"publish": "publish_decision", "stop": END})
    g.add_edge("publish_decision", END)
    return g.compile()
