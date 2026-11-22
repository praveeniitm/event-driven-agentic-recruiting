"""Pure metric functions (no I/O)."""
from __future__ import annotations

import re

LEVELS = ["REJECT", "INTERVIEW", "STRONG_HIRE"]


def skill_prf(expected: list[str], predicted: list[str]) -> tuple[float, float, float]:
    e, p = {s.lower() for s in expected}, {s.lower() for s in predicted}
    if not e and not p:
        return 1.0, 1.0, 1.0
    tp = len(e & p)
    precision = tp / len(p) if p else 0.0
    recall = tp / len(e) if e else 1.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return precision, recall, f1


def recommendation_distance(expected: str, predicted: str) -> int:
    return abs(LEVELS.index(expected) - LEVELS.index(predicted))


def pii_leaks(text: str, name: str, email: str, phone: str) -> list[str]:
    """Return PII strings still present in `text` (case-insensitive)."""
    low = text.lower()
    leaks = []
    if email and email.lower() in low:
        leaks.append("email")
    digits = re.sub(r"\D", "", phone)[-10:]  # last 10 digits so a leak without the country code is still caught
    if digits and digits in re.sub(r"\D", "", text):
        leaks.append("phone")
    for part in name.split():
        if len(part) >= 3 and re.search(rf"\b{re.escape(part.lower())}\b", low):
            leaks.append(f"name:{part}")
    return leaks


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0
