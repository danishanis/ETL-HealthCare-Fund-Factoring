-- Drop entire schemas if they exist (cassade to drop all dependent objects)
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS stg CASCADE;
DROP SCHEMA IF EXISTS core CASCADE;
DROP SCHEMA IF EXISTS marts CASCADE;

-- === Schemas ===
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

-- RAW TABLES (land CSVs)
DROP TABLE IF EXISTS raw.bank_transactions;
CREATE TABLE raw.bank_transactions (
    txn_id        TEXT PRIMARY KEY,
    txn_date      DATE,
    amount        NUMERIC,
    currency      TEXT,
    description   TEXT,
    account_id    TEXT
);

DROP TABLE IF EXISTS raw.policy_commissions;
CREATE TABLE raw.policy_commissions (
    policy_id           TEXT,
    facility_id         TEXT,
    insurer             TEXT,
    insured_name        TEXT,
    policy_number       TEXT,
    commission_due_date DATE,
    commission_amount   NUMERIC,
    written_date        DATE,
    status              TEXT,
    currency            TEXT,
    expected_bank_memo  TEXT,
    PRIMARY KEY (policy_id, commission_due_date)
);

DROP TABLE IF EXISTS raw.facilities;
CREATE TABLE raw.facilities (
    facility_id             TEXT PRIMARY KEY,
    counterparty_name       TEXT,
    max_advance_rate        NUMERIC,
    reserve_rate            NUMERIC,
    concentration_limit_pct NUMERIC,
    delinquency_cutoff_days INT,
    current_outstanding     NUMERIC,
    currency                TEXT
);

-- STG TABLES (typed/cleaned versions)
DROP TABLE IF EXISTS stg.bank_transactions_clean;
CREATE TABLE stg.bank_transactions_clean (
    txn_id        TEXT PRIMARY KEY,
    txn_date      DATE,
    amount        NUMERIC(18,2),
    currency      TEXT,
    description   TEXT,
    insurer       TEXT,
    policy_number TEXT,
    account_id    TEXT
);

DROP TABLE IF EXISTS stg.policy_commissions_clean;
CREATE TABLE stg.policy_commissions_clean (
    policy_id           TEXT,
    facility_id         TEXT,
    insurer             TEXT,
    insured_name        TEXT,
    policy_number       TEXT,
    commission_due_date DATE,
    commission_amount   NUMERIC(18,2),
    written_date        DATE,
    status              TEXT,
    currency            TEXT,
    expected_bank_memo  TEXT,
    PRIMARY KEY (policy_id, commission_due_date)
);

DROP TABLE IF EXISTS stg.facilities_clean;
CREATE TABLE stg.facilities_clean (
    facility_id             TEXT PRIMARY KEY,
    counterparty_name       TEXT,
    max_advance_rate        NUMERIC(5,4),   -- rates like 0.85
    reserve_rate            NUMERIC(5,4),
    concentration_limit_pct NUMERIC(5,2),
    delinquency_cutoff_days INT,
    current_outstanding     NUMERIC(18,2),
    currency                TEXT
);

-- CORE TABLES (final reconciled results)
DROP TABLE IF EXISTS core.reconciled_collections;
CREATE TABLE core.reconciled_collections (
    policy_id        TEXT,
    commission_due_date DATE,
    expected_amount  NUMERIC(18,2),
    received_date    DATE,
    received_amount  NUMERIC(18,2),
    shortfall        NUMERIC(18,2),
    dpd              INT,
    match_confidence NUMERIC(5,2)
);

-- MARTS (GOLD/final reporting outputs)
DROP TABLE IF EXISTS marts.borrowing_base_summary;
CREATE TABLE marts.borrowing_base_summary (
    facility_id         TEXT,
    as_of_date          DATE,
    total_eligible      NUMERIC(18,2),
    total_after_conc    NUMERIC(18,2),
    advance             NUMERIC(18,2),
    reserve             NUMERIC(18,2),
    borrowing_base      NUMERIC(18,2),
    headroom            NUMERIC(18,2)
);

DROP TABLE IF EXISTS marts.exceptions;
CREATE TABLE marts.exceptions (
    exception_type  TEXT,
    policy_id       TEXT,
    insurer         TEXT,
    commission_due_date DATE,
    expected_amount NUMERIC(18,2),
    received_date   DATE,
    received_amount NUMERIC(18,2),
    notes           TEXT
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