# src/exports.py
import os
import pandas as pd
from sqlalchemy import create_engine

WAREHOUSE_URI = os.environ.get(
    "WAREHOUSE_CONN_URI",
    "postgresql+psycopg2://warehouse_user:warehouse_pass@postgres:5432/warehouse"
)

OUT_DIR = "/opt/data/outputs"

def export_marts_to_csv():
    """
    Export marts tables to CSVs under /opt/data/outputs (mounted to ./data/outputs).
    """
    engine = create_engine(WAREHOUSE_URI)
    queries = {
        "borrowing_base_summary": "SELECT * FROM marts.borrowing_base_summary ORDER BY facility_id, as_of_date",
        "exceptions": "SELECT * FROM marts.exceptions ORDER BY exception_type, policy_id, commission_due_date",
        "reconciled_collections": "SELECT * FROM core.reconciled_collections ORDER BY policy_id, commission_due_date",
        "run_audit": "SELECT * FROM marts.run_audit ORDER BY run_id DESC",
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    for name, sql in queries.items():
        df = pd.read_sql_query(sql, engine)
        out_path = os.path.join(OUT_DIR, f"{name}.csv")
        df.to_csv(out_path, index=False)
        print(f"Wrote {len(df):,} rows to {out_path}")


if __name__ == "__main__":
    export_marts_to_csv()