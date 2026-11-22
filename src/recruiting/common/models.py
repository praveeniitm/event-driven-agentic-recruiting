"""Pydantic mirrors of the Avro contracts in /schemas.

Avro is the wire contract (enforced by Schema Registry). Pydantic gives validation and typing in code.
tests/test_contracts.py checks that the two stay in sync.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def anonymize_email(email: str) -> str:
    # PROD: HMAC-SHA256 with a secret pepper so the hash cannot be brute-forced from a list of emails.
    return hashlib.sha256(email.strip().lower().encode()).hexdigest()[:32]


class JobStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class Recommendation(str, Enum):
    STRONG_HIRE = "STRONG_HIRE"
    INTERVIEW = "INTERVIEW"
    REJECT = "REJECT"


class JobRequisition(BaseModel):
    job_id: str
    title: str
    department: str
    required_skills: list[str]
    min_experience_years: int = Field(ge=0)
    status: JobStatus = JobStatus.OPEN
    description: str = ""
    updated_at: str = Field(default_factory=utc_now_iso)


class CandidateApplication(BaseModel):
    application_id: str
    job_id: str
    candidate_name: str
    candidate_email: str
    raw_resume_text: str
    timestamp: str = Field(default_factory=utc_now_iso)


class ParsedResume(BaseModel):
    application_id: str
    job_id: str
    anonymized_id: str
    extracted_skills: list[str]
    total_experience_years: float = Field(ge=0)
    summary: str
    model: str = ""
    prompt_version: str = ""
    parsed_at: str = Field(default_factory=utc_now_iso)


class CandidateScore(BaseModel):
    application_id: str
    job_id: str
    anonymized_id: str
    fit_score: int = Field(ge=0, le=100)
    recommendation: Recommendation
    reasoning: str
    suggested_interview_questions: list[str]
    job_title: str = ""
    model: str = ""
    prompt_version: str = ""
    scored_at: str = Field(default_factory=utc_now_iso)


class DeadLetter(BaseModel):
    application_id: str
    job_id: str | None = None
    failed_service: str
    source_topic: str
    source_partition: int
    source_offset: int
    error_type: str
    error_message: str
    error_trace: str
    attempts: int
    original_payload: str
    failed_at: str = Field(default_factory=utc_now_iso)


class PiiAuditRecord(BaseModel):
    application_id: str
    anonymized_id: str
    redacted_fields: list[str]
    raw_pii: dict[str, str]
    redaction_count: int
    audited_at: str = Field(default_factory=utc_now_iso)


class JobStats(BaseModel):
    job_id: str
    candidates_scored: int
    avg_fit_score: float
    strong_hire_count: int
    interview_count: int
    reject_count: int
    updated_at: str = Field(default_factory=utc_now_iso)
