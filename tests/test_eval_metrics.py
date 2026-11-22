from recruiting.eval import metrics as m
from recruiting.eval.run import evaluate


def test_skill_prf():
    p, r, f1 = m.skill_prf(["Python", "Kafka"], ["python", "Go"])
    assert (p, r) == (0.5, 0.5) and round(f1, 2) == 0.5
    assert m.skill_prf([], []) == (1.0, 1.0, 1.0)


def test_pii_leak_detection():
    assert m.pii_leaks("contact jane at x", "Jane Doe", "j@x.com", "") == ["name:Jane"]
    assert "phone" in m.pii_leaks("call 4155550100", "", "", "+1 (415) 555-0100")
    assert m.pii_leaks("clean text", "Jane Doe", "j@x.com", "+1 415 555 0100") == []


def test_recommendation_distance():
    assert m.recommendation_distance("REJECT", "STRONG_HIRE") == 2


def test_offline_eval_runs_and_meets_gates_with_mock_baseline():
    rep = evaluate()
    assert rep["failures"] == [], rep["failures"]
    assert rep["summary"]["parser.pii_leaks"] == 0
