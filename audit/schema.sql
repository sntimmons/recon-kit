-- audit/schema.sql
-- Drop and recreate tables each run (simple, idempotent).

DROP TABLE IF EXISTS matched_pairs_raw;
DROP TABLE IF EXISTS finalized_pairs;

CREATE TABLE matched_pairs_raw (
    pair_id               TEXT PRIMARY KEY,
    match_source          TEXT,
    match_key             TEXT,
    old_worker_id         TEXT,
    new_worker_id         TEXT,
    old_recon_id          TEXT,
    new_recon_id          TEXT,
    old_full_name_norm    TEXT,
    new_full_name_norm    TEXT,
    old_dob               TEXT,
    new_dob               TEXT,
    old_last4_ssn         TEXT,
    new_last4_ssn         TEXT,
    old_position          TEXT,
    new_position          TEXT,
    old_hire_date         TEXT,
    new_hire_date         TEXT,
    old_worker_status     TEXT,
    new_worker_status     TEXT,
    old_worker_type       TEXT,
    new_worker_type       TEXT,
    old_district          TEXT,
    new_district          TEXT,
    old_location_state    TEXT,
    new_location_state    TEXT,
    old_location          TEXT,
    new_location          TEXT,
    old_salary            TEXT,
    new_salary            TEXT,
    old_payrate           TEXT,
    new_payrate           TEXT,
    inserted_at_utc       TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_matched_old_worker   ON matched_pairs_raw(old_worker_id);
CREATE INDEX IF NOT EXISTS idx_matched_new_worker   ON matched_pairs_raw(new_worker_id);
CREATE INDEX IF NOT EXISTS idx_matched_match_source ON matched_pairs_raw(match_source);

-- matched_pairs view: always provides pair_id even before resolve adds it.
-- load_sqlite.py's _create_views() will replace this with a smarter version
-- at runtime, but having it here lets schema.sql stay self-contained.
DROP VIEW IF EXISTS matched_pairs;
CREATE VIEW matched_pairs AS
SELECT
  rowid AS pair_id,
  *
FROM matched_pairs_raw;

CREATE TABLE finalized_pairs (
    candidate_id          TEXT PRIMARY KEY,
    old_worker_id         TEXT,
    new_worker_id         TEXT,
    old_recon_id          TEXT,
    new_recon_id          TEXT,
    old_full_name_norm    TEXT,
    new_full_name_norm    TEXT,
    old_dob               TEXT,
    new_dob               TEXT,
    last4_ssn             TEXT,
    confidence            TEXT,
    score                 TEXT,
    name_similarity       TEXT,
    decision              TEXT,
    notes                 TEXT,
    inserted_at_utc       TEXT DEFAULT (datetime('now'))
);
