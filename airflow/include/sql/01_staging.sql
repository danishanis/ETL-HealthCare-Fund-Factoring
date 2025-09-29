-- TRANSFORM RAW → STAGING

-- Truncate staging tables before reload
TRUNCATE TABLE stg.bank_transactions_clean CASCADE;
TRUNCATE TABLE stg.policy_commissions_clean CASCADE;
TRUNCATE TABLE stg.facilities_clean CASCADE;

-- BANK TRANSACTIONS
INSERT INTO stg.bank_transactions_clean (
    txn_id,
    txn_date,
    amount,
    currency,
    description,
    insurer,
    policy_number,
    account_id
)
SELECT
    txn_id,
    txn_date::DATE,
    amount::NUMERIC,
    currency,
    description,
    -- Example parsing: "DEP Aviva COMM POL1007"
    NULLIF(split_part(description, ' ', 2), '') AS insurer,
    regexp_replace(description, '.*(POL[0-9]+).*', '\1') AS policy_number,
    account_id
FROM raw.bank_transactions;

-- POLICY COMMISSIONS
INSERT INTO stg.policy_commissions_clean (
    policy_id,
    facility_id,
    insurer,
    insured_name,
    policy_number,
    commission_due_date,
    commission_amount,
    written_date,
    status,
    currency,
    expected_bank_memo
)
SELECT
    policy_id,
    facility_id,
    insurer,
    insured_name,
    policy_number,
    commission_due_date::DATE,
    commission_amount::NUMERIC,
    written_date::DATE,
    status,
    currency,
    expected_bank_memo
FROM raw.policy_commissions;

-- FACILITIES
INSERT INTO stg.facilities_clean (
    facility_id,
    counterparty_name,
    max_advance_rate,
    reserve_rate,
    concentration_limit_pct,
    delinquency_cutoff_days,
    current_outstanding,
    currency
)
SELECT
    facility_id,
    counterparty_name,
    max_advance_rate::NUMERIC,
    reserve_rate::NUMERIC,
    concentration_limit_pct::NUMERIC,
    delinquency_cutoff_days::INT,
    current_outstanding::NUMERIC,
    currency
FROM raw.facilities;