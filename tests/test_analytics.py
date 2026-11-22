from recruiting.services.analytics.aggregate import update


def score(app, fit, rec, job="J1"):
    return {"application_id": app, "job_id": job, "fit_score": fit, "recommendation": rec}


def test_running_aggregate():
    state, s = update(None, score("a", 90, "STRONG_HIRE"))
    state, s = update(state, score("b", 50, "INTERVIEW"))
    state, s = update(state, score("c", 10, "REJECT"))
    assert (s.candidates_scored, s.avg_fit_score) == (3, 50.0)
    assert (s.strong_hire_count, s.interview_count, s.reject_count) == (1, 1, 1)


def test_duplicate_delivery_is_idempotent_and_rescore_overwrites():
    state, _ = update(None, score("a", 90, "STRONG_HIRE"))
    state, s = update(state, score("a", 90, "STRONG_HIRE"))
    assert s.candidates_scored == 1
    state, s = update(state, score("a", 30, "REJECT"))
    assert s.candidates_scored == 1 and s.reject_count == 1 and s.strong_hire_count == 0
