-- MARTS: REPORTING LAYER

-- Truncate marts tables before reload
TRUNCATE TABLE marts.borrowing_base_summary CASCADE;
TRUNCATE TABLE marts.exceptions CASCADE;
TRUNCATE TABLE marts.run_audit CASCADE;

-- BORROWING BASE SUMMARY
INSERT INTO marts.borrowing_base_summary (
    facility_id,
    as_of_date,
    total_eligible,
    total_after_conc,
    advance,
    reserve,
    borrowing_base,
    headroom
)
WITH eligible AS (
    SELECT
        pc.facility_id,
        pc.policy_id,
        pc.commission_due_date,
        pc.commission_amount
    FROM stg.policy_commissions_clean pc
    LEFT JOIN core.reconciled_collections rc
           ON pc.policy_id = rc.policy_id
          AND pc.commission_due_date = rc.commission_due_date
    JOIN stg.facilities_clean f
            ON pc.facility_id = f.facility_id
    WHERE (rc.received_date IS NULL OR rc.dpd <= f.delinquency_cutoff_days)
)
, eligible_sum AS (
    SELECT
        f.facility_id,
        SUM(e.commission_amount) AS total_eligible,
        f.max_advance_rate,
        f.reserve_rate,
        f.concentration_limit_pct,
        f.current_outstanding
    FROM eligible e
    JOIN stg.facilities_clean f ON e.facility_id = f.facility_id
    GROUP BY f.facility_id, f.max_advance_rate, f.reserve_rate, f.concentration_limit_pct, f.current_outstanding
)
, conc_limited AS (
    SELECT
        e.facility_id,
        LEAST(SUM(e.commission_amount),
              es.total_eligible * es.concentration_limit_pct / 100.0) AS eligible_after_conc
    FROM eligible e
    JOIN eligible_sum es ON e.facility_id = es.facility_id
    GROUP BY e.facility_id, es.total_eligible, es.concentration_limit_pct
)
SELECT
    es.facility_id,
    CURRENT_DATE AS as_of_date,
    es.total_eligible,
    cl.eligible_after_conc,
    es.total_eligible * es.max_advance_rate AS advance,
    es.total_eligible * es.reserve_rate AS reserve,
    (es.total_eligible * es.max_advance_rate) - (es.total_eligible * es.reserve_rate) AS borrowing_base,
    ((es.total_eligible * es.max_advance_rate) - (es.total_eligible * es.reserve_rate)) - es.current_outstanding AS headroom
FROM eligible_sum es
JOIN conc_limited cl ON es.facility_id = cl.facility_id;

-- EXCEPTIONS
INSERT INTO marts.exceptions (
    exception_type,
    policy_id,
    insurer,
    commission_due_date,
    expected_amount,
    received_date,
    received_amount,
    notes
)
SELECT
    CASE
        WHEN rc.received_date IS NULL AND rc.dpd > f.delinquency_cutoff_days THEN 'Late'
        WHEN rc.received_amount < rc.expected_amount * 0.98 THEN 'Short Pay'
        WHEN rc.received_amount > rc.expected_amount * 1.02 THEN 'Over Pay'
        WHEN rc.match_confidence < 1.0 THEN 'Unmatched'
    END AS exception_type,
    rc.policy_id,
    pc.insurer,
    rc.commission_due_date,
    rc.expected_amount,
    rc.received_date,
    rc.received_amount,
    CONCAT('DPD=', rc.dpd, ', Confidence=', rc.match_confidence)
FROM core.reconciled_collections rc
JOIN stg.policy_commissions_clean pc
     ON rc.policy_id = pc.policy_id
LEFT JOIN stg.facilities_clean f
     ON pc.facility_id = f.facility_id
WHERE rc.received_date IS NULL
   OR rc.received_amount <> rc.expected_amount
   OR rc.match_confidence < 1.0;

-- RUN AUDIT
INSERT INTO marts.run_audit (
    records_raw,
    records_stg,
    records_core,
    notes
)
SELECT
    (SELECT COUNT(*) FROM raw.policy_commissions) +
    (SELECT COUNT(*) FROM raw.bank_transactions) +
    (SELECT COUNT(*) FROM raw.facilities),
    (SELECT COUNT(*) FROM stg.policy_commissions_clean) +
    (SELECT COUNT(*) FROM stg.bank_transactions_clean) +
    (SELECT COUNT(*) FROM stg.facilities_clean),
    (SELECT COUNT(*) FROM core.reconciled_collections),
    'ETL run completed successfully';
