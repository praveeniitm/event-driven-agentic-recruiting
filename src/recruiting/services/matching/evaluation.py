"""Structured-output schema, prompt and offline heuristic for candidate-vs-job evaluation."""
from __future__ import annotations

from typing import Sequence

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from recruiting.common.models import Recommendation

PROMPT_VERSION = "matching-v1"

STRONG_HIRE_MIN = 85
INTERVIEW_MIN = 45

SYSTEM_PROMPT = f"""You are a technical recruiter evaluating one anonymised candidate against one job requisition.
Judge ONLY on skills and years of experience given. Do not infer or use gender, age, ethnicity, nationality or any
other protected attribute. Do not invent facts that are not in the profile.

Return:
- fit_score: integer 0-100. Weigh required-skill coverage about 70 percent and experience versus the minimum about 30 percent.
- recommendation: STRONG_HIRE if fit_score >= {STRONG_HIRE_MIN}, INTERVIEW if fit_score >= {INTERVIEW_MIN}, otherwise REJECT.
- reasoning: 2-4 sentences naming which required skills match, which are missing, and how experience compares.
- suggested_interview_questions: exactly 2 tailored questions. Probe a claimed strength and a gap or risk."""


class Evaluation(BaseModel):
    fit_score: int = Field(ge=0, le=100)
    recommendation: Recommendation
    reasoning: str
    suggested_interview_questions: list[str] = Field(min_length=2, max_length=2)


def recommendation_for(score: int) -> Recommendation:
    if score >= STRONG_HIRE_MIN:
        return Recommendation.STRONG_HIRE
    return Recommendation.INTERVIEW if score >= INTERVIEW_MIN else Recommendation.REJECT


def build_messages(job: dict, resume: dict) -> list[BaseMessage]:
    human = (
        f"JOB\nTitle: {job['title']}\nDepartment: {job['department']}\n"
        f"Required skills: {', '.join(job['required_skills'])}\n"
        f"Minimum experience (years): {job['min_experience_years']}\nDescription: {job.get('description', '')}\n\n"
        f"CANDIDATE (anonymised id {resume['anonymized_id']})\n"
        f"Skills: {', '.join(resume['extracted_skills'])}\n"
        f"Total experience (years): {resume['total_experience_years']}\nSummary: {resume['summary']}"
    )
    return [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=human)]


def _norm(s: str) -> str:
    return s.strip().lower()


def mock_evaluate(messages: Sequence[BaseMessage]) -> Evaluation:
    """Offline baseline: 70% skill coverage + 30% experience ratio. Reads the same prompt text the LLM would."""
    text = str(messages[-1].content)
    required = [s.strip() for s in text.split("Required skills:")[1].split("\n")[0].split(",") if s.strip()]
    min_years = float(text.split("Minimum experience (years):")[1].split("\n")[0])
    skills = [s.strip() for s in text.split("Skills:")[-1].split("\n")[0].split(",") if s.strip()]
    years = float(text.split("Total experience (years):")[1].split("\n")[0])

    have = {_norm(s) for s in skills}
    matched = [r for r in required if _norm(r) in have]
    missing = [r for r in required if _norm(r) not in have]
    skill_ratio = len(matched) / len(required) if required else 1.0
    exp_ratio = min(years / min_years, 1.0) if min_years > 0 else 1.0
    score = round(70 * skill_ratio + 30 * exp_ratio)

    strength = matched[0] if matched else (skills[0] if skills else "their background")
    gap = missing[0] if missing else "scaling this work to production"
    reasoning = (f"Matches {len(matched)} of {len(required)} required skills ({', '.join(matched) or 'none'}); "
                 f"missing: {', '.join(missing) or 'none'}. Has {years:g} years of experience versus a minimum of {min_years:g}.")
    return Evaluation(
        fit_score=score, recommendation=recommendation_for(score), reasoning=reasoning,
        suggested_interview_questions=[
            f"Walk us through the most complex system you built using {strength}.",
            f"You have limited exposure to {gap}. How would you get productive with it in your first month?",
        ],
    )
