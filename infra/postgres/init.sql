-- Tables are created up front (auto.create=false on the connectors) so the schema is reviewed, not guessed by a connector.
-- PROD: manage with a migration tool (Flyway / Alembic) and give the connector a role with INSERT/UPDATE only.

CREATE TABLE IF NOT EXISTS candidate_scores (
    application_id                 TEXT PRIMARY KEY,           -- upsert key: Connect is at-least-once, so replays must be idempotent
    job_id                         TEXT NOT NULL,
    anonymized_id                  TEXT NOT NULL,
    fit_score                      INTEGER NOT NULL CHECK (fit_score BETWEEN 0 AND 100),
    recommendation                 TEXT NOT NULL CHECK (recommendation IN ('STRONG_HIRE','INTERVIEW','REJECT')),
    reasoning                      TEXT NOT NULL,
    suggested_interview_questions  TEXT[] NOT NULL,
    job_title                      TEXT,
    model                          TEXT,
    prompt_version                 TEXT,
    scored_at                      TEXT
);
CREATE INDEX IF NOT EXISTS idx_scores_job ON candidate_scores (job_id, fit_score DESC);

-- Both parser-dlq and matching-dlq land here. Primary key is the Kafka coordinates (pk.mode=kafka).
CREATE TABLE IF NOT EXISTS dead_letters (
    __connect_topic      TEXT    NOT NULL,
    __connect_partition  INTEGER NOT NULL,
    __connect_offset     BIGINT  NOT NULL,
    application_id       TEXT    NOT NULL,
    job_id               TEXT,
    failed_service       TEXT    NOT NULL,
    source_topic         TEXT    NOT NULL,
    source_partition     INTEGER NOT NULL,
    source_offset        BIGINT  NOT NULL,
    error_type           TEXT    NOT NULL,
    error_message        TEXT    NOT NULL,
    error_trace          TEXT    NOT NULL,
    attempts             INTEGER NOT NULL,
    original_payload     TEXT    NOT NULL,   -- may contain PII. PROD: encrypt or drop this column from the sink.
    failed_at            TEXT    NOT NULL,
    PRIMARY KEY (__connect_topic, __connect_partition, __connect_offset)
);

CREATE TABLE IF NOT EXISTS job_stats (
    job_id             TEXT PRIMARY KEY,
    candidates_scored  INTEGER NOT NULL,
    avg_fit_score      DOUBLE PRECISION NOT NULL,
    strong_hire_count  INTEGER NOT NULL,
    interview_count    INTEGER NOT NULL,
    reject_count       INTEGER NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_requisitions (
    job_id                TEXT PRIMARY KEY,
    title                 TEXT NOT NULL,
    department            TEXT NOT NULL,
    required_skills       TEXT[] NOT NULL,
    min_experience_years  INTEGER NOT NULL,
    status                TEXT NOT NULL,
    description           TEXT,
    updated_at            TEXT
);
