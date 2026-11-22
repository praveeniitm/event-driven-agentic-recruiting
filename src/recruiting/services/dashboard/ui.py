"""Gradio dashboard. Tab 1 monitors the pipeline (Postgres), Tab 2 produces events (Kafka)."""
from __future__ import annotations

import functools

import gradio as gr
import pandas as pd

from recruiting.common.models import JobRequisition, JobStatus
from recruiting.services.dashboard import db
from recruiting.services.producer.producer import EventProducer

REFRESH_SECONDS = 5
SCORE_COLS = ["application_id", "job_id", "job_title", "fit_score", "recommendation", "model", "scored_at"]
EMPTY_NOTE = "No data yet. Is the stack running and has the pipeline processed anything?"


@functools.lru_cache(maxsize=1)
def _producer() -> EventProducer:
    return EventProducer()


def _df(rows: list[dict], cols: list[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    return df[cols] if cols and not df.empty else df


# ---------- Tab 1: monitor ----------
def refresh(job_filter: str):
    try:
        scores = _df(db.fetch_scores(job_filter), SCORE_COLS)
        dlq = db.fetch_dead_letters()
        stats = _df(db.fetch_job_stats())
        job_ids = ["ALL"] + [j["job_id"] for j in db.fetch_jobs()]
    except Exception as exc:  # DB down must not crash the page
        msg = f"Database unavailable: {exc}"
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), msg, gr.update()
    banner = (f"**{len(dlq)} message(s) in the dead-letter queues.** Review them below." if dlq
              else "No dead letters. The pipeline is healthy.")
    return scores, _df(dlq), stats, banner, gr.update(choices=job_ids)


def show_detail(evt: gr.SelectData) -> str:
    app_id = evt.row_value[0]
    d = db.fetch_score_detail(app_id)
    if not d:
        return "Not found."
    questions = "\n".join(f"{i}. {q}" for i, q in enumerate(d["suggested_interview_questions"], 1))
    return (f"### {d['job_title']}: {d['recommendation']} ({d['fit_score']}/100)\n"
            f"**Anonymized candidate:** `{d['anonymized_id']}`  \n"
            f"**Reasoning:** {d['reasoning']}\n\n**Suggested interview questions**\n\n{questions}\n\n"
            f"*model `{d['model']}`, prompt `{d['prompt_version']}`, application `{d['application_id']}`*")


# ---------- Tab 2: hiring manager portal ----------
def load_job(job_id: str):
    job = next((j for j in db.fetch_jobs() if j["job_id"] == job_id), None)
    if not job:
        return [gr.update()] * 7
    return (job["job_id"], job["title"], job["department"], ", ".join(job["required_skills"]),
            job["min_experience_years"], job["status"], job["description"] or "")


def publish_job(job_id, title, department, skills, min_years, status, description):
    skill_list = [s.strip() for s in (skills or "").split(",") if s.strip()]
    if not job_id.strip() or not title.strip() or not skill_list:
        return "job_id, title and at least one skill are required."
    try:
        job = JobRequisition(job_id=job_id.strip(), title=title.strip(), department=department.strip() or "General",
                             required_skills=skill_list, min_experience_years=int(min_years or 0),
                             status=JobStatus(status), description=description or "")
        _producer().publish_job(job)
    except Exception as exc:
        return f"Failed: {exc}"
    return (f"Published {job.job_id} ({job.status.value}) to `job-requisitions`. Because the topic is compacted, "
            f"only this latest version is kept per job_id.")


def submit_resume(job_id, name, email, text):
    if not (job_id and name.strip() and email.strip() and text.strip()):
        return "Job, name, email and resume text are required."
    try:
        app = _producer().submit_resume(job_id, name.strip(), email.strip(), text)
    except Exception as exc:
        return f"Failed: {exc}"
    return (f"Submitted application `{app.application_id}`. Watch the Live Pipeline Monitor tab. "
            f"Resumes shorter than 50 characters will land in the DLQ.")


def build_app() -> gr.Blocks:
    with gr.Blocks(title="Agentic Recruiting Pipeline") as demo:
        gr.Markdown("# Event-Driven Agentic Recruiting\nKafka + LangGraph + bytewax. Data below is read from Postgres, "
                    "written by Kafka Connect.")
        with gr.Tab("Live Pipeline Monitor"):
            banner = gr.Markdown()
            job_filter = gr.Dropdown(["ALL"], value="ALL", label="Filter by job")
            gr.Markdown("### Candidate scores (click a row for reasoning and interview questions)")
            scores = gr.Dataframe(interactive=False, wrap=True)
            detail = gr.Markdown()
            gr.Markdown("### Per-job statistics (bytewax)")
            stats = gr.Dataframe(interactive=False)
            gr.Markdown("### Dead-letter queue")
            dlq = gr.Dataframe(interactive=False, wrap=True)
            timer = gr.Timer(REFRESH_SECONDS)
            outputs = [scores, dlq, stats, banner, job_filter]
            timer.tick(refresh, inputs=job_filter, outputs=outputs)
            demo.load(refresh, inputs=job_filter, outputs=outputs)
            scores.select(show_detail, outputs=detail)

        with gr.Tab("Hiring Manager Portal"):
            gr.Markdown("### Create or update a job requisition")
            existing = gr.Dropdown([], label="Load existing job to edit (optional)", allow_custom_value=True)
            with gr.Row():
                f_id = gr.Textbox(label="Job ID", value="JOB-2001")
                f_title = gr.Textbox(label="Title")
                f_dept = gr.Textbox(label="Department", value="Engineering")
            f_skills = gr.Textbox(label="Required skills (comma separated)")
            with gr.Row():
                f_years = gr.Number(label="Minimum experience (years)", value=3, precision=0)
                f_status = gr.Dropdown([s.value for s in JobStatus], value="OPEN", label="Status")
            f_desc = gr.Textbox(label="Description", lines=2)
            job_btn = gr.Button("Publish job requisition", variant="primary")
            job_out = gr.Markdown()
            job_form = [f_id, f_title, f_dept, f_skills, f_years, f_status, f_desc]
            existing.change(load_job, existing, job_form)
            job_btn.click(publish_job, job_form, job_out)

            gr.Markdown("### Submit a test resume")
            r_job = gr.Dropdown([], label="Job", allow_custom_value=True)
            with gr.Row():
                r_name = gr.Textbox(label="Candidate name")
                r_email = gr.Textbox(label="Candidate email")
            r_text = gr.Textbox(label="Resume text", lines=8)
            r_btn = gr.Button("Submit application", variant="primary")
            r_out = gr.Markdown()
            r_btn.click(submit_resume, [r_job, r_name, r_email, r_text], r_out)

            def job_choices():
                try:
                    ids = [j["job_id"] for j in db.fetch_jobs()]
                except Exception:
                    ids = []
                return gr.update(choices=ids), gr.update(choices=ids)

            demo.load(job_choices, outputs=[existing, r_job])
            job_btn.click(job_choices, outputs=[existing, r_job])
    return demo
