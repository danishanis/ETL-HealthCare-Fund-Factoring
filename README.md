# Data Engineering Case-Study - Factoring Fund ETL MVP

This project implements an **ETL pipeline** for a factoring fund that advances cash against future insurance receivables. The pipeline ingests insurer/broker commission statements and bank transaction exports, reconciles expected vs received cash, computes borrowing base availability per facility, and produces exception reports for monitoring delinquencies.  

The entire workflow is orchestrated using **Apache Airflow** and persists data into a **Postgres warehouse**. Both services run in **Docker** for portability and reproducibility.

---

## 📖 Problem Background

The factoring company needs to:  

1. **Reconcile expected vs actual cash**  
   - Match policy commissions (expected receivables) against deposits in bank transactions.  
   - Handle late payments, short-pays (tolerance = 2%), and over-pays.  
   - Record unmatched items as exceptions.  

2. **Compute as-of-date borrowing base** per facility  
   - Eligible receivables = expected but uncollected items, not past `delinquency_cutoff_days`.  
   - Apply `concentration_limit_pct` per insurer.  
   - Advance = `eligible_sum * max_advance_rate`.  
   - Reserve = `eligible_sum * reserve_rate`.  
   - Borrowing base = Advance − Reserve.  
   - Headroom = Borrowing base − Current outstanding.  

3. **Produce exception reports and metrics**  
   - Late, short-pay, over-pay, unmatched transactions.  
   - Daily borrowing base summary.  
   - ETL run audit trail.  

---

## 📊 Assumptions

- **Tolerance for short/over-pay**: 2% of expected commission.  
- **As-of date**: `min(today, latest bank transaction date)`.  
- **Deposit splitting**: unmatched deposits remain flagged.  
- **Over-payments**: treated as misallocations to preserve a clean audit trail.  
- **Database**: Postgres only (no DuckDB).  
- **Composite PK**: `(policy_id, commission_due_date)` for `policy_commissions`.  

---

## 🏗️ Solution Design

The pipeline follows a layered architecture:

1. **Raw layer (`raw.*`)**  
   - Directly ingests CSV files (`facilities.csv`, `policy_commissions.csv`, `bank_transactions.csv`).  
   - Preserves all fields as-is.  

2. **Staging layer (`stg.*`)**  
   - Applies type casting (dates, numerics).  
   - Parses insurer and policy numbers from bank transaction descriptions.  
   - Cleans status and currency fields.  

3. **Core layer (`core.*`)**  
   - Reconciles expected vs received commissions.  
   - Computes shortfall, days past due (DPD), and match confidence.  

4. **Marts layer (`marts.*`)**  
   - `borrowing_base_summary` — facility-level borrowing base with intermediate calculations.  
   - `exceptions` — all mismatches (late, short, over-pay, unmatched).  
   - `run_audit` — record counts across all layers for auditability.  

---

## 📂 Repository Structure

```
take-home/
│
├── airflow/
│   ├── dags/
│   │   ├── factoring_etl_dag.py     # Main Airflow DAG
│   │   └── check_files_dag.py       # Utility DAG to confirm CSV visibility
│   ├── include/sql/
│   │   ├── 00_bootstrap.sql         # Create schemas and raw/stg/core/marts tables
│   │   ├── 01_staging.sql           # Raw → Staging transformations
│   │   ├── 02_core.sql              # Reconciliation logic
│   │   └── 03_marts.sql             # Borrowing base + exceptions + audit
│   └── logs/                        # Airflow logs (gitignored)
│
├── data/input/
│   ├── bank_transactions.csv
│   ├── facilities.csv
│   └── policy_commissions.csv
│
├── docs/
│   └── factoring_etl_dag.png        # DAG graph screenshot from Airflow UI
│
├── src/
│   └── ingestion.py                 # CSV ingestion helper
│
├── docker-compose.yml               # Docker services for Airflow + Postgres
├── requirements.txt                 # Python dependencies
└── README.md                        # This document
```

---

## 🚀 Quick Start

### 1. Clone the repo
```bash
git clone https://github.com/danishanis/ETL-HealthCare-Fund-Factoring.git
cd ETL-HealthCare-Fund-Factoring
```

### 2. Start Docker services
```bash
docker compose down --volumes --remove-orphans
docker compose build --no-cache
docker compose up -d
```

This will start:
- `postgres` → the warehouse database  
- `airflow-webserver` → Airflow UI at [http://localhost:8080](http://localhost:8080)  
- `airflow-scheduler` → orchestrates DAG runs  

### 3. Access Airflow
- URL: [http://localhost:8080](http://localhost:8080)  
- Default login:  
  - **Username**: `admin`  
  - **Password**: `admin` (set via `docker-compose.yml`, change if needed)  

### 4. Run the pipeline
1. In the Airflow UI, enable the DAG **`factoring_etl`**.  
2. Trigger a manual run (▶ button).  
3. Watch logs for each step:  
   - `bootstrap_db` → sets up schemas/tables.  
   - `load_*` → ingests raw CSVs.  
   - `transform_staging` → cleans/types data.  
   - `transform_core` → reconciles expected vs received cash.  
   - `transform_marts` → produces borrowing base, exceptions, audit.  

### 5. Validate results
Connect to Postgres and check row counts:

```bash
docker compose exec postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM raw.bank_transactions;"
docker compose exec postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM stg.policy_commissions_clean;"
docker compose exec postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM core.reconciled_collections;"
docker compose exec postgres psql -U postgres -d warehouse -c "SELECT * FROM marts.run_audit ORDER BY run_id DESC LIMIT 1;"
```

---

## ✅ Deliverables

- **Airflow DAG** orchestrating the ETL pipeline.  
- **Postgres warehouse** with layered schemas (`raw`, `stg`, `core`, `marts`).  
- **Reconciliation logic** with tolerance, short/over-pay handling, and exceptions.  
- **Borrowing base calculation** with concentration and delinquency rules.  
- **Audit logs** of every run.  

---

## ⚠️ Notes for Interviewers

- **Tolerance (Y%)**: 2%.  
- **As-of date**: `min(today, last bank transaction date)`.  
- **Over-payments**: flagged as misallocations.  
- **Deposit splitting**: unmatched deposits remain flagged.  
- **DB**: Postgres only, no DuckDB.  
