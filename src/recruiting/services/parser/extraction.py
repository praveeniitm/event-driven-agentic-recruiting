"""Structured-output schema, prompt and offline heuristic for resume profile extraction."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Sequence

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, Field

PROMPT_VERSION = "parser-v1"  # bump on every prompt change so eval results and scores are traceable to a prompt

SYSTEM_PROMPT = """You extract structured data from an anonymised resume. Personal data has already been replaced \
by placeholders like [NAME], [EMAIL], [PHONE], [ADDRESS]. Never guess or reconstruct those values.

Return:
- extracted_skills: concrete technical skills and tools stated in the resume (e.g. "Python", "Kafka"). \
Use canonical names, no duplicates, no soft skills.
- total_experience_years: total years of professional experience as a number. Prefer an explicit statement, \
otherwise derive it from the job date ranges. Do not count education. Use 0 if there is no professional experience.
- summary: exactly two sentences describing the candidate's seniority, domain and strongest skills."""


class ExtractedProfile(BaseModel):
    """What the LLM is asked to produce. IDs and lineage fields are added by the service, never by the model."""

    extracted_skills: list[str] = Field(description="Canonical technical skills found in the resume")
    total_experience_years: float = Field(ge=0, le=60, description="Total professional experience in years")
    summary: str = Field(description="Exactly two sentences summarising the candidate")


def build_messages(redacted_text: str) -> list[BaseMessage]:
    return [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=f"Resume:\n\n{redacted_text}")]


SKILL_VOCAB = [
    "Python", "Go", "Java", "TypeScript", "JavaScript", "Bash", "SQL", "Kafka", "PostgreSQL", "Redis", "Docker",
    "Kubernetes", "Terraform", "Ansible", "Linux", "Prometheus", "Grafana", "Nagios", "AWS", "gRPC", "REST",
    "Flask", "Django", "FastAPI", "PyTorch", "scikit-learn", "XGBoost", "pandas", "LLMs", "RAG", "MLOps", "MLflow",
    "Kubeflow", "React", "CSS", "Figma", "Jest", "pytest",
]


def mock_extract(messages: Sequence[BaseMessage]) -> ExtractedProfile:
    """Rule-based stand-in for the LLM: keyword skills, explicit/derived years, first two summary sentences."""
    text = str(messages[-1].content)
    skills = [s for s in SKILL_VOCAB if re.search(rf"(?<![\w-]){re.escape(s)}(?![\w-])", text, re.IGNORECASE)]

    years = 0.0
    explicit = re.findall(r"(\d+(?:\.\d+)?)\+?\s*years?", text, re.IGNORECASE)
    if explicit:
        years = max(float(y) for y in explicit)
    else:
        this_year = datetime.now().year
        spans = re.findall(r"\b((?:19|20)\d{2})\s*[-\u2013]\s*((?:19|20)\d{2}|present)\b", text, re.IGNORECASE)
        if spans:
            starts = [int(a) for a, _ in spans]
            ends = [this_year if b.lower() == "present" else int(b) for _, b in spans]
            years = float(max(ends) - min(starts))

    m = re.search(r"SUMMARY\s*\n(.+?)(?:\n\s*\n|\n[A-Z]{4,}\b)", text, re.DOTALL)
    sentences = re.split(r"(?<=[.!?])\s+", (m.group(1) if m else text[:300]).strip())
    summary = " ".join(s.strip() for s in sentences[:2]) or "No summary available."
    return ExtractedProfile(extracted_skills=skills, total_experience_years=years, summary=summary)
