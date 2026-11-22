"""LangGraph workflow for the parser agent.

START -> redact_pii --(error)--> END
              |
              v
        extract_profile -> END

Errors do not raise out of the graph. They are written to state["error"] so the Kafka layer can
route the original message to parser-dlq and still commit the offset.
"""
from __future__ import annotations

import time
import traceback
from typing import Callable, TypedDict

from langchain_core.runnables import Runnable
from langgraph.graph import END, START, StateGraph

from recruiting.common import metrics
from recruiting.common.llm import LLMError, call_llm
from recruiting.services.parser.extraction import ExtractedProfile, build_messages
from recruiting.services.parser.pii import RedactionResult, redact

MIN_RESUME_CHARS = 50
MAX_LLM_ATTEMPTS = 2  # "LLM fails twice" -> DLQ


class ParserError(TypedDict):
    type: str
    message: str
    trace: str


class ParserState(TypedDict, total=False):
    application: dict
    redaction: RedactionResult
    profile: ExtractedProfile
    attempts: int
    error: ParserError | None


def build_parser_graph(llm: Runnable, sleep: Callable[[float], None] = time.sleep):
    def redact_pii(state: ParserState) -> ParserState:
        app = state["application"]
        raw = (app.get("raw_resume_text") or "").strip()
        if len(raw) < MIN_RESUME_CHARS:
            return {"error": {"type": "RESUME_TOO_SHORT",
                              "message": f"resume has {len(raw)} chars, minimum is {MIN_RESUME_CHARS}", "trace": ""}}
        result = redact(raw, app.get("candidate_name", ""), app.get("candidate_email", ""))
        return {"redaction": result, "error": None}

    def route_after_redact(state: ParserState) -> str:
        return "failed" if state.get("error") else "ok"

    def extract_profile(state: ParserState) -> ParserState:
        messages = build_messages(state["redaction"].text)
        last_trace = ""
        last_exc: Exception = LLMError("no attempt made")
        for attempt in range(1, MAX_LLM_ATTEMPTS + 1):
            try:
                profile = call_llm(llm, messages, "parser")
                assert isinstance(profile, ExtractedProfile)
                if not profile.summary.strip():
                    raise LLMError("empty summary")
                # Defence in depth: the model must not re-introduce PII, so scrub its output with the same rules.
                app = state["application"]
                scrubbed = redact(profile.summary, app.get("candidate_name", ""), app.get("candidate_email", ""))
                if scrubbed.count:
                    metrics.DISCARDED.labels("parser", "pii_in_llm_output").inc()
                    profile = profile.model_copy(update={"summary": scrubbed.text})
                profile = profile.model_copy(update={
                    "extracted_skills": list(dict.fromkeys(s.strip() for s in profile.extracted_skills if s.strip()))
                })
                return {"profile": profile, "attempts": attempt, "error": None}
            except Exception as exc:
                last_trace = traceback.format_exc()
                if attempt < MAX_LLM_ATTEMPTS:
                    sleep(0.5 * attempt)  # PROD: exponential backoff + jitter, and honour 429 Retry-After.
                last_exc = exc
        return {"attempts": MAX_LLM_ATTEMPTS,
                "error": {"type": "LLM_FAILURE", "message": str(last_exc), "trace": last_trace}}

    g = StateGraph(ParserState)
    g.add_node("redact_pii", redact_pii)
    g.add_node("extract_profile", extract_profile)
    g.add_edge(START, "redact_pii")
    g.add_conditional_edges("redact_pii", route_after_redact, {"ok": "extract_profile", "failed": END})
    g.add_edge("extract_profile", END)
    return g.compile()
