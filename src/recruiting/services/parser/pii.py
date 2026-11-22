"""Deterministic PII redaction. Runs BEFORE any text is sent to an LLM.

Why rules and not the LLM: PII removal must be reliable and auditable, and sending raw PII to a
third-party model defeats the purpose. We also know the exact name and email from the event itself.
PROD: use Microsoft Presidio or a NER model (names/addresses in free text are hard for regexes),
and measure recall on a labelled set (the eval harness has a PII-leak check for this).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_CANDIDATE_RE = re.compile(r"\+?\(?\d[\d\s().\-]{7,}\d")
URL_RE = re.compile(r"(?:https?://|www\.)\S+|(?:linkedin|github)\.com/\S+", re.IGNORECASE)
ADDRESS_LINE_RE = re.compile(r"^(\s*(?:address|location|residence)\s*:\s*)(.+)$", re.IGNORECASE | re.MULTILINE)
STREET_RE = re.compile(
    r"\b\d{1,5}\s+(?:[A-Z][\w.]*\s+){1,4}(?:Street|St|Avenue|Ave|Road|Rd|Lane|Ln|Drive|Dr|Boulevard|Blvd|Way)\b[^\n]*"
)
YEAR_RANGE_RE = re.compile(r"^\d{4}\s*[-\u2013]\s*\d{4}$")


@dataclass
class RedactionResult:
    text: str
    found: dict[str, list[str]] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return sum(len(v) for v in self.found.values())

    @property
    def kinds(self) -> list[str]:
        return sorted(self.found)


def _add(found: dict[str, list[str]], kind: str, value: str) -> None:
    value = value.strip()
    if value and value not in found.setdefault(kind, []):
        found[kind].append(value)


def redact(text: str, candidate_name: str = "", candidate_email: str = "") -> RedactionResult:
    found: dict[str, list[str]] = {}

    def sub(pattern: re.Pattern[str], kind: str, text: str) -> str:
        def repl(m: re.Match[str]) -> str:
            _add(found, kind, m.group(0))
            return f"[{kind}]"
        return pattern.sub(repl, text)

    # 1. Emails (regex catches any email, including ones that are not the candidate's own).
    text = sub(EMAIL_RE, "EMAIL", text)
    if candidate_email:
        text = re.sub(re.escape(candidate_email), "[EMAIL]", text, flags=re.IGNORECASE)

    # 2. URLs (LinkedIn / GitHub profiles identify a person).
    text = sub(URL_RE, "URL", text)

    # 3. Phones: loose candidate match, then validate by digit count so "2020-2021" is not treated as a phone.
    def phone_repl(m: re.Match[str]) -> str:
        raw = m.group(0)
        digits = re.sub(r"\D", "", raw)
        if not 9 <= len(digits) <= 15 or YEAR_RANGE_RE.match(raw.strip()):
            return raw
        _add(found, "PHONE", raw)
        return "[PHONE]"
    text = PHONE_CANDIDATE_RE.sub(phone_repl, text)

    # 4. Addresses: explicit "Address:" lines first, then street-shaped strings anywhere.
    def addr_line_repl(m: re.Match[str]) -> str:
        _add(found, "ADDRESS", m.group(2))
        return f"{m.group(1)}[ADDRESS]"
    text = ADDRESS_LINE_RE.sub(addr_line_repl, text)
    text = sub(STREET_RE, "ADDRESS", text)

    # 5. Names: the full name plus each name part (3+ letters) so "Emily" alone is caught too.
    if candidate_name.strip():
        parts = [candidate_name.strip()] + [p for p in re.split(r"\s+", candidate_name.strip()) if len(p) >= 3]
        for part in sorted(set(parts), key=len, reverse=True):
            pattern = re.compile(rf"\b{re.escape(part)}\b", re.IGNORECASE)
            if pattern.search(text):
                _add(found, "NAME", part)
                text = pattern.sub("[NAME]", text)

    return RedactionResult(text=text, found=found)
