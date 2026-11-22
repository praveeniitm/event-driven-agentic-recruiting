"""Sample data: 3 org job roles and 10 resumes (one intentionally corrupted to trigger the DLQ).

Run:  uv run python -m recruiting.services.producer.seed_data [--demo-compaction]

Each resume has an `expected` label. Kafka never sees it, the eval harness (eval/) uses it as ground truth.
"""
from __future__ import annotations

import argparse
import uuid

from recruiting.common.models import CandidateApplication, JobRequisition
from recruiting.services.producer.producer import EventProducer

JOBS = [
    JobRequisition(
        job_id="JOB-1001",
        title="Senior Backend Engineer",
        department="Platform Engineering",
        required_skills=["Python", "Kafka", "PostgreSQL", "Docker"],
        min_experience_years=5,
        description="Own event-driven backend services that process millions of events per day.",
    ),
    JobRequisition(
        job_id="JOB-1002",
        title="Machine Learning Engineer",
        department="Applied AI",
        required_skills=["Python", "PyTorch", "LLMs", "MLOps"],
        min_experience_years=3,
        description="Build and ship LLM-powered features, evaluate them, and run them in production.",
    ),
    JobRequisition(
        job_id="JOB-1003",
        title="Site Reliability Engineer",
        department="Infrastructure",
        required_skills=["Kubernetes", "Terraform", "Linux", "Prometheus"],
        min_experience_years=4,
        description="Keep the platform reliable: SLOs, incident response, infrastructure as code.",
    ),
]


def _resume(name: str, email: str, phone: str, address: str, body: str) -> dict:
    header = f"{name}\nEmail: {email} | Phone: {phone}\nAddress: {address}\n\n"
    return {"name": name, "email": email, "phone": phone, "address": address, "text": header + body.strip()}


RESUMES = [
    {
        "job_id": "JOB-1001", "expected": "STRONG_HIRE",
        **_resume(
            "Aarav Mehta", "aarav.mehta@example.com", "+1 (415) 555-0142", "214 Market Street, San Francisco, CA 94105",
            """
SUMMARY
Backend engineer with 8 years of experience building streaming systems.

EXPERIENCE
Staff Engineer, Streamly Inc. (2020-present): Designed Kafka-based event pipelines in Python handling 40k msg/s.
Operated PostgreSQL clusters and shipped everything in Docker on Kubernetes.
Senior Engineer, DataHub (2016-2020): Built REST and gRPC services in Python and Go, mentored 4 engineers.

SKILLS
Python, Kafka, PostgreSQL, Docker, Kubernetes, Go, gRPC, Redis""",
        ),
    },
    {
        "job_id": "JOB-1001", "expected": "INTERVIEW",
        **_resume(
            "Emily Carter", "emily.carter@example.com", "+1 (212) 555-0178", "88 Hudson Avenue, New York, NY 10014",
            """
SUMMARY
Software engineer with 4 years of Python web development.

EXPERIENCE
Software Engineer, ShopLine (2021-present): Built Flask APIs backed by PostgreSQL, containerised with Docker.
Junior Developer, WebWorks (2020-2021): Maintained Django apps and wrote integration tests.

SKILLS
Python, Flask, Django, PostgreSQL, Docker, REST, pytest. No production Kafka experience yet.""",
        ),
    },
    {
        "job_id": "JOB-1001", "expected": "REJECT",
        **_resume(
            "Marcus Johnson", "marcus.johnson@example.com", "+1 (312) 555-0119", "1500 Lake Shore Drive, Chicago, IL 60610",
            """
SUMMARY
Frontend developer with 6 years of experience building web interfaces.

EXPERIENCE
Senior Frontend Developer, PixelForge (2018-present): React and TypeScript design systems, accessibility audits.
Frontend Developer, AdBright (2017-2018): Built landing pages with HTML, CSS and JavaScript.

SKILLS
React, TypeScript, JavaScript, CSS, Figma, Jest""",
        ),
    },
    {
        "job_id": "JOB-1002", "expected": "STRONG_HIRE",
        **_resume(
            "Sofia Rossi", "sofia.rossi@example.com", "+44 20 7946 0958", "12 Baker Lane, London NW1 6XE, United Kingdom",
            """
SUMMARY
ML engineer with 5 years of experience taking LLM and deep learning models to production.

EXPERIENCE
ML Engineer, Lingua AI (2021-present): Fine-tuned LLMs with PyTorch, built RAG pipelines, set up evaluation harnesses
and MLOps (MLflow, Kubeflow, CI for models). Reduced inference cost by 40 percent.
Data Scientist, RetailMind (2019-2021): Demand forecasting models in Python and PyTorch.

SKILLS
Python, PyTorch, LLMs, RAG, MLOps, MLflow, Kubeflow, SQL""",
        ),
    },
    {
        "job_id": "JOB-1002", "expected": "INTERVIEW",
        **_resume(
            "Wei Zhang", "wei.zhang@example.com", "+1 (206) 555-0166", "455 Pine Street, Seattle, WA 98101",
            """
SUMMARY
Data scientist with 3 years of experience in classical machine learning.

EXPERIENCE
Data Scientist, CloudCart (2021-present): Churn models with scikit-learn and XGBoost, some PyTorch experiments,
deployed models as FastAPI services. Limited exposure to LLMs.
Data Analyst, CloudCart (2020-2021): SQL dashboards and A/B tests.

SKILLS
Python, scikit-learn, XGBoost, PyTorch (basic), SQL, FastAPI""",
        ),
    },
    {
        "job_id": "JOB-1002", "expected": "REJECT",
        **_resume(
            "Priya Nair", "priya.nair@example.com", "+91 98765 43210", "7 Lotus Road, Bengaluru, Karnataka 560001, India",
            """
SUMMARY
Recent graduate looking for a first role in machine learning.

EXPERIENCE
Intern, Campus Analytics Lab (summer 2024): Cleaned datasets and trained a simple logistic regression model.

EDUCATION
B.Tech Computer Science, 2024. Coursework: statistics, intro to machine learning.

SKILLS
Python, pandas, scikit-learn""",
        ),
    },
    {
        "job_id": "JOB-1003", "expected": "STRONG_HIRE",
        **_resume(
            "Daniel Okafor", "daniel.okafor@example.com", "+1 (512) 555-0133", "902 Congress Avenue, Austin, TX 78701",
            """
SUMMARY
Site reliability engineer with 7 years of experience running production Kubernetes platforms.

EXPERIENCE
Senior SRE, FinEdge (2019-present): Managed 30 Kubernetes clusters with Terraform, defined SLOs, built Prometheus
and Grafana alerting, led incident response on Linux fleets.
Systems Engineer, NetCore (2017-2019): Linux administration and CI/CD automation.

SKILLS
Kubernetes, Terraform, Linux, Prometheus, Grafana, Python, AWS""",
        ),
    },
    {
        "job_id": "JOB-1003", "expected": "INTERVIEW",
        **_resume(
            "Laura Schmidt", "laura.schmidt@example.com", "+49 30 5557 0188", "Torstrasse 45, 10119 Berlin, Germany",
            """
SUMMARY
Systems administrator with 5 years of experience moving toward cloud and SRE work.

EXPERIENCE
Systems Administrator, MediaGroup (2020-present): Linux servers, Ansible automation, Docker, Nagios monitoring.
Started learning Kubernetes and Terraform in side projects.

SKILLS
Linux, Ansible, Docker, Nagios, Bash, Python""",
        ),
    },
    {
        "job_id": "JOB-1003", "expected": "REJECT",
        **_resume(
            "Tom Becker", "tom.becker@example.com", "+1 (303) 555-0105", "77 Pearl Street, Denver, CO 80203",
            """
SUMMARY
IT support specialist with 1 year of experience.

EXPERIENCE
Help Desk Technician, OfficeNet (2024-present): Password resets, laptop setup, ticket triage.

SKILLS
Windows, Microsoft Office, ticketing systems""",
        ),
    },
    {
        # Intentionally corrupted: empty resume body. The parser must send this to parser-dlq.
        "job_id": "JOB-1001", "expected": "DLQ",
        "name": "Corrupted Candidate", "email": "corrupted.candidate@example.com", "phone": "", "address": "",
        "text": "",
    },
]


def build_applications() -> list[CandidateApplication]:
    apps = []
    for r in RESUMES:
        # uuid5 makes the id deterministic, so re-seeding does not create new "candidates" (idempotent demo).
        app_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{r['email']}|{r['job_id']}"))
        apps.append(
            CandidateApplication(
                application_id=app_id, job_id=r["job_id"], candidate_name=r["name"],
                candidate_email=r["email"], raw_resume_text=r["text"],
            )
        )
    return apps


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo-compaction", action="store_true",
                        help="publish several edits of JOB-1001 so you can watch compaction keep only the latest")
    parser.add_argument("--jobs-only", action="store_true")
    args = parser.parse_args()

    producer = EventProducer()
    for job in JOBS:
        producer.publish_job(job)

    if args.demo_compaction:
        job = JOBS[0]
        for years in (6, 7, 5):
            producer.publish_job(job.model_copy(update={"min_experience_years": years}))

    if not args.jobs_only:
        for app in build_applications():
            producer.publish_application(app)


if __name__ == "__main__":
    main()
