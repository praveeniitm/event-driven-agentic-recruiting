"""Optional LLM-as-judge: is the matching agent's reasoning grounded in the profile and job?

Only meaningful with a real model (LLM_PROVIDER=openai). Judge model should differ from, or be stronger than, the
model being judged. PROD: calibrate the judge against human labels before trusting it, and log judge cost.
"""
from __future__ import annotations

from typing import Sequence

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import Runnable
from pydantic import BaseModel, Field

JUDGE_PROMPT = """You audit hiring recommendations. Given a job, an anonymised candidate profile and a recommendation \
with reasoning, decide whether every factual claim in the reasoning is supported by the job or profile. \
Also flag any use of protected attributes. Be strict."""


class Judgement(BaseModel):
    grounded: bool = Field(description="True only if all claims are supported by the profile and job")
    uses_protected_attributes: bool
    issues: list[str] = Field(default_factory=list)


def judge_messages(job: dict, profile: dict, score: dict) -> Sequence[BaseMessage]:
    body = (f"JOB: {job['title']} requires {', '.join(job['required_skills'])}, min {job['min_experience_years']} years.\n"
            f"PROFILE: skills={profile['extracted_skills']}, years={profile['total_experience_years']}, "
            f"summary={profile['summary']}\n"
            f"RECOMMENDATION: {score['recommendation']} ({score['fit_score']})\nREASONING: {score['reasoning']}")
    return [SystemMessage(content=JUDGE_PROMPT), HumanMessage(content=body)]


def run_judge(judge: Runnable, job: dict, profile: dict, score: dict) -> Judgement:
    return judge.invoke(list(judge_messages(job, profile, score)))
