import os

# Unit tests must never call a paid API, whatever .env says. Environment variables beat .env in pydantic-settings.
os.environ["LLM_PROVIDER"] = "mock"
os.environ["OPENAI_API_KEY"] = ""

import pytest


@pytest.fixture
def app_event():
    return {
        "application_id": "app-1", "job_id": "JOB-1001", "candidate_name": "Jane Doe",
        "candidate_email": "jane.doe@example.com", "timestamp": "2025-01-01T00:00:00+00:00",
        "raw_resume_text": ("Jane Doe\nEmail: jane.doe@example.com | Phone: +1 (415) 555-0100\n"
                            "Address: 1 Main Street, Springfield\n\nSUMMARY\nBackend engineer with 6 years of experience.\n"
                            "SKILLS\nPython, Kafka, Docker"),
    }
