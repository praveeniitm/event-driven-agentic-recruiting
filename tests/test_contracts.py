"""Static checks that keep Avro schemas, pydantic models, topic specs and connector configs consistent."""
import json
from pathlib import Path

import pytest

from recruiting.common import models, topics

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
SPEC = json.loads((ROOT / "infra" / "topics.json").read_text())

SCHEMA_TO_MODEL = {
    "job_requisition.avsc": models.JobRequisition, "candidate_application.avsc": models.CandidateApplication,
    "parsed_resume.avsc": models.ParsedResume, "candidate_score.avsc": models.CandidateScore,
    "dead_letter.avsc": models.DeadLetter, "pii_audit.avsc": models.PiiAuditRecord, "job_stats.avsc": models.JobStats,
}


@pytest.mark.parametrize("schema_file,model", SCHEMA_TO_MODEL.items())
def test_avro_fields_match_pydantic_fields(schema_file, model):
    avro_fields = {f["name"] for f in json.loads((SCHEMAS / schema_file).read_text())["fields"]}
    assert avro_fields == set(model.model_fields), schema_file


@pytest.mark.parametrize("schema_file,model", SCHEMA_TO_MODEL.items())
def test_avro_schema_is_parseable(schema_file, model):
    import fastavro
    fastavro.parse_schema(json.loads((SCHEMAS / schema_file).read_text()))


def test_topic_spec_matches_prd():
    by_name = {t["name"]: t for t in SPEC["topics"]}
    assert by_name["job-requisitions"]["partitions"] == 2 and by_name["job-requisitions"]["config"]["cleanup.policy"] == "compact"
    assert by_name["candidate-applications"]["partitions"] == 3
    assert by_name["parsed-resumes"]["partitions"] == 3
    assert by_name["parser-dlq"]["partitions"] == 1
    assert by_name["candidate-scores"]["partitions"] == 3


def test_every_python_topic_constant_is_declared_and_schema_mapped():
    declared = {t["name"]: t.get("value_schema") for t in SPEC["topics"]}
    for name, schema in topics.VALUE_SCHEMA_FILES.items():
        assert declared[name] == schema
        assert (SCHEMAS / schema).exists()


def test_connectors_are_valid_and_point_at_declared_topics():
    declared = {t["name"] for t in SPEC["topics"]}
    for f in (ROOT / "infra" / "connect" / "connectors").glob("*.json"):
        cfg = json.loads(f.read_text())
        assert set(cfg["topics"].split(",")) <= declared, f.name
        assert cfg["auto.create"] == "false"


def test_seed_data_has_3_jobs_10_resumes_one_corrupted():
    from recruiting.services.producer.seed_data import JOBS, RESUMES
    assert len(JOBS) == 3 and len(RESUMES) == 10
    assert sum(1 for r in RESUMES if len(r["text"].strip()) < 50) == 1


def test_anonymized_id_is_stable_and_case_insensitive():
    assert models.anonymize_email("A@b.com ") == models.anonymize_email("a@B.com")
