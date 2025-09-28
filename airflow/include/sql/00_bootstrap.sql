-- === Schemas ===
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

-- === RAW TABLES (land as-is, types are TEXT for safety) ===
DROP TABLE IF EXISTS raw.bank_transactions;
CREATE TABLE raw.bank_transactions (
    txn_id           SERIAL PRIMARY KEY,
    txn_date         TEXT,
    amount           TEXT,
    description      TEXT
);

DROP TABLE IF EXISTS raw.policy_commissions;
CREATE TABLE raw.policy_commissions (
    policy_id        TEXT,
    policy_number    TEXT,
    insurer          TEXT,
    expected_date    TEXT,
    expected_amount  TEXT,
    facility_id      TEXT
);

DROP TABLE IF EXISTS raw.facilities;
CREATE TABLE raw.facilities (
    facility_id              TEXT PRIMARY KEY,
    delinquency_cutoff_days  TEXT,
    concentration_limit_pct  TEXT,
    max_advance_rate         TEXT,
    reserve_rate             TEXT,
    current_outstanding      TEXT
);

-- === STG TABLES (typed/cleaned versions) ===
-- We’ll populate these later with transforms.
DROP TABLE IF EXISTS stg.bank_txn_clean;
CREATE TABLE stg.bank_txn_clean (
    txn_id        INT PRIMARY KEY,
    txn_date      DATE,
    amount        NUMERIC,
    description   TEXT,
    insurer       TEXT,
    policy_number TEXT
);

DROP TABLE IF EXISTS stg.policy_commissions_clean;
CREATE TABLE stg.policy_commissions_clean (
    policy_id       TEXT,
    policy_number   TEXT,
    insurer         TEXT,
    expected_date   DATE,
    expected_amount NUMERIC,
    facility_id     TEXT
);

DROP TABLE IF EXISTS stg.facilities_clean;
CREATE TABLE stg.facilities_clean (
    facility_id              TEXT PRIMARY KEY,
    delinquency_cutoff_days  INT,
    concentration_limit_pct  NUMERIC,
    max_advance_rate         NUMERIC,
    reserve_rate             NUMERIC,
    current_outstanding      NUMERIC
);

-- === CORE TABLES (final reconciled results) ===
DROP TABLE IF EXISTS core.reconciled_collections;
CREATE TABLE core.reconciled_collections (
    policy_id       TEXT,
    expected_date   DATE,
    expected_amount NUMERIC,
    received_date   DATE,
    received_amount NUMERIC,
    shortfall       NUMERIC,
    dpd             INT,
    match_confidence NUMERIC
);

-- === MARTS (final reporting outputs) ===
DROP TABLE IF EXISTS marts.borrowing_base_summary;
CREATE TABLE marts.borrowing_base_summary (
    facility_id         TEXT,
    as_of_date          DATE,
    total_eligible      NUMERIC,
    total_after_conc    NUMERIC,
    advance             NUMERIC,
    reserve             NUMERIC,
    borrowing_base      NUMERIC,
    headroom            NUMERIC
);

DROP TABLE IF EXISTS marts.exceptions;
CREATE TABLE marts.exceptions (
    exception_type      TEXT,
    policy_id           TEXT,
    insurer             TEXT,
    expected_date       DATE,
    expected_amount     NUMERIC,
    received_date       DATE,
    received_amount     NUMERIC,
    notes               TEXT
);

DROP TABLE IF EXISTS marts.run_audit;
CREATE TABLE marts.run_audit (
    run_id        SERIAL PRIMARY KEY,
    run_date      TIMESTAMP DEFAULT now(),
    records_raw   INT,
    records_stg   INT,
    records_core  INT,
    notes         TEXT
);