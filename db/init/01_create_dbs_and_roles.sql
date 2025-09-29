-- Create roles/users (safe if rerun)
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
      CREATE ROLE airflow_user LOGIN PASSWORD 'airflow_pass';
   END IF;
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'warehouse_user') THEN
      CREATE ROLE warehouse_user LOGIN PASSWORD 'warehouse_pass';
   END IF;
END;
$$;

-- Create databases owned by the right roles
-- We cannot do this inside DO, so use IF NOT EXISTS directly
CREATE DATABASE airflow   WITH OWNER = airflow_user   TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';
CREATE DATABASE warehouse WITH OWNER = warehouse_user TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';

-- (Optional) Grants — usually redundant if OWNER set
GRANT CONNECT ON DATABASE airflow   TO airflow_user;
GRANT CONNECT ON DATABASE warehouse TO warehouse_user;