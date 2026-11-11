CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ctgov_studies (
    nct_id TEXT NOT NULL,
    source_updated_date DATE NOT NULL,
    payload JSONB NOT NULL,
    batch_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (nct_id, source_updated_date)
);

CREATE TABLE IF NOT EXISTS raw.faers_reports (
    report_id TEXT NOT NULL,
    report_version TEXT NOT NULL,
    payload JSONB NOT NULL,
    batch_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_id, report_version)
);
