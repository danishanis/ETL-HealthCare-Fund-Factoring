# Senior Data Engineer Take‑Home (Factoring Fund ETL MVP)

This project involves creating an ETL pipelie using Apache Airflow. The pipeline extracts data from ... (Add transformation details here)... and loads it into a Postgres database. The entire workflow is orchestrated by Airflow, a platform that allows scheduling, monitoring and managing workflows.

The project leverages Docker to run Airflow and Postgres as services, ensuring an isloated and reproducible environment. 

This folder only contains sample data and a minimal Docker/Python scaffold. The full assignment brief is in the document you received.

## Structure
- `data/` — synthetic CSVs: `facilities.csv`, `policy_commissions.csv`, `bank_transactions.csv`
- `requirements.txt`, `Dockerfile`, `docker-compose.yml` — optional scaffolding
- (you will create) `etl/` — your pipeline code, and `sql/` for analytics queries

## Quick start
```bash
docker compose build
docker compose run --rm app python -V
```

You can use Postgres (included) or DuckDB if you prefer local files. Replace the `app` command in `docker-compose.yml` with your flow entrypoint (e.g., Prefect).

Good luck!
