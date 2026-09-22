CREATE TABLE IF NOT EXISTS validation_results (
    message_id TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL,
    status TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS validation_results_endpoint_id_idx
    ON validation_results (endpoint_id);
