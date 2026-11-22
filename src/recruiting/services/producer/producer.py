"""Event producer: publish JobRequisition and CandidateApplication events as Avro."""
from __future__ import annotations

import uuid

from recruiting.common import topics
from recruiting.common.kafka import AvroProducer
from recruiting.common.logging import get_logger
from recruiting.common.models import CandidateApplication, JobRequisition, JobStatus, utc_now_iso

log = get_logger("producer")


class EventProducer:
    def __init__(self, producer: AvroProducer | None = None) -> None:
        self._producer = producer or AvroProducer("event-producer")

    def publish_job(self, job: JobRequisition) -> None:
        """Key = job_id on a compacted topic, so only the latest version per job survives compaction."""
        job = job.model_copy(update={"updated_at": utc_now_iso()})
        self._producer.produce(topics.JOB_REQUISITIONS, key=job.job_id, value=job.model_dump(mode="json"))
        log.info("job published", extra={"job_id": job.job_id, "status": job.status.value})

    def close_job(self, job: JobRequisition) -> None:
        # A tombstone (null value) would delete the key on compaction and consumers could not tell "closed" from "unknown".
        self.publish_job(job.model_copy(update={"status": JobStatus.CLOSED}))

    def publish_application(self, app: CandidateApplication) -> None:
        """Key = job_id so all applications for a job land on one partition (ordering per job)."""
        self._producer.produce(
            topics.CANDIDATE_APPLICATIONS,
            key=app.job_id,
            value=app.model_dump(mode="json"),
            headers={"correlation-id": app.application_id},
        )
        log.info("application published", extra={"application_id": app.application_id, "job_id": app.job_id})

    def submit_resume(self, job_id: str, name: str, email: str, resume_text: str) -> CandidateApplication:
        app = CandidateApplication(
            application_id=str(uuid.uuid4()),
            job_id=job_id,
            candidate_name=name,
            candidate_email=email,
            raw_resume_text=resume_text,
        )
        self.publish_application(app)
        return app
