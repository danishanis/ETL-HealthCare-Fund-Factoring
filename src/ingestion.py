import pandas as pd
from sqlalchemy import create_engine, text
import os

def load_csv_postgres(table_name, file_name):
    """
    Generic Loader: reads a CSV from /opt/data/input/<file_name> and writes it
    into raw.<table_name> in the Postgres warehouse DB.
    """

    conn_uri = os.environ.get(
        "WAREHOUSE_CONN_URI",
        "postgresql+psycopg2://warehouse_user:warehouse_pass@postgres:5432/warehouse"
    )
    engine = create_engine(conn_uri)

    csv_path = f"/opt/data/input/{file_name}"
    df = pd.read_csv(csv_path)

    if "date" in df.columns and "txn_date" not in df.columns:
        df.rename(columns={"date": "txn_date"}, inplace=True)

    # Get table schema columns
    with engine.begin() as conn:

        # Getting the target table column from Postgres
        query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw' 
                AND table_name = :tbl
            ORDER BY ordinal_position
        """)

        result = conn.execute(query, {"tbl": table_name})
        table_cols = [row[0] for row in result]

        # print(f"Schema columns for raw.{table_name}: {table_cols}")

    # Ensure all required columns exist in csvs
    missing_cols = [c for c in table_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"CSV file {file_name} is missing required columns: {missing_cols}"
        )
    
    # Keeping only the matchin columns
    df = df[table_cols]

    # Truncate table
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE raw.{table_name} CASCADE;"))
    
    # Insert the data 
    with engine.begin() as conn:
        df.to_sql(
            table_name,
            con=conn,
            schema="raw",
            if_exists="append", index=False,
            method='multi',
            chunksize=1000
        )
        
    print(f"Loaded {len(df)} rows into raw.{table_name} from {file_name}")

###########################################################

# import os
# import pandas as pd
# import psycopg2

# def load_csv_postgres(table_name, file_name):
#     """
#     Generic Loader: reads a CSV from /opt/data/input/<file_name> and writes it
#     into raw.<table_name> in the Postgres warehouse DB.
#     """

#     # Construct the DSN for psycopg2
#     dsn = f"dbname='{os.environ['WAREHOUSE_DB_NAME']}' " \
#           f"user='{os.environ['WAREHOUSE_DB_USER']}' " \
#           f"password='{os.environ['WAREHOUSE_DB_PASSWORD']}' " \
#           f"host='postgres' port='5432'"

#     csv_path = f"/opt/data/input/{file_name}"
#     df = pd.read_csv(csv_path)

#     # Get table schema columns
#     with psycopg2.connect(dsn) as conn:
#         with conn.cursor() as cursor:
#             # Getting the target table column from Postgres
#             cursor.execute("""
#                 SELECT column_name
#                 FROM information_schema.columns
#                 WHERE table_schema = 'raw' 
#                     AND table_name = %s
#                 ORDER BY ordinal_position
#             """, (table_name,))
#             table_cols = [row[0] for row in cursor.fetchall()]

#             print(f"Schema columns for raw.{table_name}: {table_cols}")

#             # Ensure all required columns exist in csvs
#             missing_cols = [c for c in table_cols if c not in df.columns]
#             if missing_cols:
#                 raise ValueError(
#                     f"CSV file {file_name} is missing required columns: {missing_cols}"
#                 )
            
#             # Keeping only the matching columns
#             df = df[table_cols]

#             # Truncate & insert
#             cursor.execute(f"TRUNCATE TABLE raw.{table_name} CASCADE;")

#             # Insert using psycopg2
#             for index, row in df.iterrows():
#                 cursor.execute(
#                     f"INSERT INTO raw.{table_name} ({', '.join(table_cols)}) VALUES ({', '.join(['%s'] * len(row))})",
#                     tuple(row)
#                 )
        
#     print(f"Loaded {len(df)} rows into raw.{table_name} from {file_name}")
