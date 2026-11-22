from langchain_core.runnables import RunnableLambda

from recruiting.services.parser.extraction import ExtractedProfile, mock_extract
from recruiting.services.parser.graph import build_parser_graph
from recruiting.services.parser.pii import redact


def make_graph(fn):
    return build_parser_graph(RunnableLambda(fn), sleep=lambda _s: None)


def test_pii_is_removed_before_llm_sees_text(app_event):
    seen = []

    def spy(messages):
        seen.append(str(messages[-1].content))
        return mock_extract(messages)

    state = make_graph(spy).invoke({"application": app_event})
    assert state["error"] is None
    sent = seen[0]
    for secret in ("Jane", "Doe", "jane.doe@example.com", "555-0100", "Main Street"):
        assert secret not in sent
    assert {"EMAIL", "PHONE", "ADDRESS", "NAME"} <= set(state["redaction"].kinds)
    assert "Python" in state["profile"].extracted_skills
    assert state["profile"].total_experience_years == 6


def test_short_resume_goes_to_error_without_calling_llm(app_event):
    app_event["raw_resume_text"] = "too short"
    called = []
    state = make_graph(lambda m: called.append(1)).invoke({"application": app_event})
    assert state["error"]["type"] == "RESUME_TOO_SHORT"
    assert not called


def test_llm_recovers_on_second_attempt(app_event):
    calls = {"n": 0}

    def flaky(messages):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("boom")
        return mock_extract(messages)

    state = make_graph(flaky).invoke({"application": app_event})
    assert state["error"] is None and state["attempts"] == 2


def test_llm_failing_twice_is_reported_with_trace(app_event):
    def always_fail(messages):
        raise TimeoutError("llm down")

    state = make_graph(always_fail).invoke({"application": app_event})
    assert state["error"]["type"] == "LLM_FAILURE"
    assert state["attempts"] == 2
    assert "llm down" in state["error"]["message"] and "Traceback" in state["error"]["trace"]


def test_pii_reintroduced_by_llm_is_scrubbed(app_event):
    def leaky(messages):
        return ExtractedProfile(extracted_skills=["Python", "Python"], total_experience_years=3,
                                summary="Jane Doe (jane.doe@example.com) is a backend engineer.")

    state = make_graph(leaky).invoke({"application": app_event})
    assert "Jane" not in state["profile"].summary and "@" not in state["profile"].summary
    assert state["profile"].extracted_skills == ["Python"]


def test_year_ranges_are_not_phone_numbers():
    res = redact("Worked 2019-2023 and then 2023 - 2025 at Acme.")
    assert res.count == 0
