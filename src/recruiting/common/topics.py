"""Topic names and the topic -> value schema file mapping (mirrors infra/topics.json)."""

JOB_REQUISITIONS = "job-requisitions"
CANDIDATE_APPLICATIONS = "candidate-applications"
PARSED_RESUMES = "parsed-resumes"
PARSER_DLQ = "parser-dlq"
MATCHING_DLQ = "matching-dlq"
CANDIDATE_SCORES = "candidate-scores"
PII_AUDIT_LOG = "pii-audit-log"
JOB_STATS = "job-stats"

VALUE_SCHEMA_FILES = {
    JOB_REQUISITIONS: "job_requisition.avsc",
    CANDIDATE_APPLICATIONS: "candidate_application.avsc",
    PARSED_RESUMES: "parsed_resume.avsc",
    PARSER_DLQ: "dead_letter.avsc",
    MATCHING_DLQ: "dead_letter.avsc",
    CANDIDATE_SCORES: "candidate_score.avsc",
    PII_AUDIT_LOG: "pii_audit.avsc",
    JOB_STATS: "job_stats.avsc",
}
