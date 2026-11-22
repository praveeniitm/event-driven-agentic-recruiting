"""Checks the OpenAI path is wired correctly without making a network call."""
import pytest

from recruiting.common.config import Settings
from recruiting.common.llm import LLMError, build_structured_llm
from recruiting.eval.judge import Judgement
from recruiting.services.matching.evaluation import Evaluation, mock_evaluate
from recruiting.services.parser.extraction import ExtractedProfile, mock_extract


@pytest.mark.parametrize("schema,mock", [(ExtractedProfile, mock_extract), (Evaluation, mock_evaluate),
                                         (Judgement, lambda m: None)])
def test_openai_runnable_builds_for_every_schema(schema, mock):
    s = Settings(llm_provider="openai", openai_api_key="sk-test-not-real", _env_file=None)
    runnable = build_structured_llm(schema, mock, s)
    assert runnable.first.kwargs.get("tools") or runnable.first.kwargs.get("functions")  # schema bound as a tool


def test_openai_without_key_fails_fast():
    with pytest.raises(LLMError):
        build_structured_llm(ExtractedProfile, mock_extract, Settings(llm_provider="openai", openai_api_key="", _env_file=None))


def test_unknown_provider_fails_fast():
    with pytest.raises(LLMError):
        build_structured_llm(ExtractedProfile, mock_extract, Settings(llm_provider="nope", _env_file=None))
