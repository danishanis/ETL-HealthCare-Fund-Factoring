-- Create application databases
-- CREATE DATABASE airflow;
-- CREATE DATABASE warehouse;

DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
        CREATE DATABASE airflow;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse') THEN
        CREATE DATABASE warehouse;
    END IF;
END
$$;

-- Create roles/users (passwords must match .env)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
        CREATE ROLE airflow_user LOGIN PASSWORD 'airflow_pass';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'warehouse_user') THEN
        CREATE ROLE warehouse_user LOGIN PASSWORD 'warehouse_pass';
    END IF;
END
$$;

-- Grant privileges on AIRFLOW DB
GRANT CONNECT ON DATABASE airflow TO airflow_user;

-- Grant privileges on WAREHOUSE DB
GRANT CONNECT ON DATABASE warehouse TO warehouse_user;

-- Ensure default privileges (schemas/tables will be created later by our bootstrap + tasks)
\connect warehouse
CREATE SCHEMA IF NOT EXISTS public AUTHORIZATION warehouse_user;
GRANT USAGE ON SCHEMA public TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO warehouse_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO warehouse_user;

\connect airflow
CREATE SCHEMA IF NOT EXISTS public AUTHORIZATION airflow_user;
GRANT USAGE ON SCHEMA public TO airflow_user