"""LLM access behind one seam so agents work with OpenAI or an offline heuristic stub.

LLM_PROVIDER=openai -> ChatOpenAI(...).with_structured_output(Schema)
LLM_PROVIDER=mock   -> deterministic rule-based function (no key, no cost). It doubles as the
                       "dumb baseline" the eval harness compares the real model against.
"""
from __future__ import annotations

import time
from typing import Callable, Sequence, TypeVar

from langchain_core.messages import BaseMessage
from langchain_core.runnables import Runnable, RunnableLambda
from pydantic import BaseModel

from recruiting.common import metrics
from recruiting.common.config import Settings, get_settings

T = TypeVar("T", bound=BaseModel)
MockFn = Callable[[Sequence[BaseMessage]], BaseModel]

MOCK_MODEL_NAME = "mock-heuristic-v1"


class LLMError(RuntimeError):
    pass


def model_name(settings: Settings | None = None) -> str:
    s = settings or get_settings()
    return s.openai_model if s.llm_provider == "openai" else MOCK_MODEL_NAME


def build_structured_llm(schema: type[T], mock_fn: MockFn, settings: Settings | None = None,
                         model: str | None = None) -> Runnable:
    s = settings or get_settings()
    if s.llm_provider == "mock":
        return RunnableLambda(mock_fn)
    if s.llm_provider == "openai":
        if not s.openai_api_key:
            raise LLMError("LLM_PROVIDER=openai but OPENAI_API_KEY is empty")
        from langchain_openai import ChatOpenAI

        # temperature=0 for repeatable output. max_retries=0 because the agent owns the retry policy (and DLQ routing).
        # PROD: add a fallback model, a circuit breaker, and per-tenant rate limits / budget caps.
        llm = ChatOpenAI(model=model or s.openai_model, api_key=s.openai_api_key, temperature=0,
                         timeout=s.llm_timeout_seconds, max_retries=0)
        # function_calling (not strict json_schema): tolerates Pydantic constraints (ge/le, list length); Pydantic re-validates the result.
        return llm.with_structured_output(schema, method="function_calling")
    raise LLMError(f"unknown LLM_PROVIDER {s.llm_provider!r}")


def call_llm(runnable: Runnable, messages: Sequence[BaseMessage], service: str) -> BaseModel:
    """Invoke with latency + outcome metrics. Any failure surfaces as LLMError."""
    start = time.perf_counter()
    try:
        result = runnable.invoke(list(messages))
        if result is None:  # with_structured_output returns None when parsing fails
            raise LLMError("model returned no structured output")
        metrics.LLM_CALLS.labels(service, "ok").inc()
        return result
    except Exception as exc:
        metrics.LLM_CALLS.labels(service, "error").inc()
        raise exc if isinstance(exc, LLMError) else LLMError(f"{type(exc).__name__}: {exc}") from exc
    finally:
        metrics.LLM_LATENCY.labels(service).observe(time.perf_counter() - start)
        # PROD: record token usage and cost per call (callbacks / LangSmith / Langfuse) and trace every prompt+response.
