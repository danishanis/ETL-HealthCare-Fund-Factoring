-- CORE: RECONCILE EXPECTED VS RECEIVED

-- Key Business Rules Implemented:
-- 1. Tolerance Y% = 2% (Assumption)
-- 2. Match Confidence Levels- 
    -- '1.0' (policy number + insurer match)
    -- '0.8' (policy number match only)
    -- '0.0' (no match)
-- 3. Shortfall Calculation: expected - received (null-safe)
-- 4. DPD (Days Past Due) Calculation:
    -- If no received_date, compare with CURRENT_DATE
    -- Else, compare received_date with commission_due_date

-- Truncate before reload
TRUNCATE TABLE core.reconciled_collections CASCADE;

-- Insert reconciled records
INSERT INTO core.reconciled_collections (
    policy_id,
    commission_due_date,
    expected_amount,
    received_date,
    received_amount,
    shortfall,
    dpd,
    match_confidence
)
SELECT
    pc.policy_id,
    pc.commission_due_date,
    pc.commission_amount,
    bt.txn_date AS received_date,
    bt.amount AS received_amount,

    -- Shortfall = expected - received (null-safe)
    COALESCE(pc.commission_amount,0) - COALESCE(bt.amount,0) AS shortfall,
    
    -- DPD = days past due (if no received_date, compare with today)
    CASE
        WHEN bt.txn_date IS NULL THEN GREATEST(0, CURRENT_DATE - pc.commission_due_date)
        ELSE GREATEST(0, bt.txn_date - pc.commission_due_date)
    END AS dpd,
    
    -- Match confidence:
    -- 1.0 = exact policy_id match
    -- 0.8 = matched by policy_number + insurer (partial)
    CASE
        WHEN bt.policy_number = pc.policy_number AND bt.insurer = pc.insurer THEN 1.0
        WHEN bt.policy_number = pc.policy_number THEN 0.8
        ELSE 0.0
    END AS match_confidence
FROM stg.policy_commissions_clean pc
LEFT JOIN stg.bank_transactions_clean bt
    ON pc.policy_number = bt.policy_number
   AND pc.insurer = bt.insurer
   AND ABS(pc.commission_amount - bt.amount) <= (pc.commission_amount * 0.02);  -- Y% = 2% tolerance
