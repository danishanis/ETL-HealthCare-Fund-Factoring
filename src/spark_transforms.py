from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs, col

def run_spark_bank_demo():
    spark = SparkSession.builder \
        .appName("BankTransactionsSparkDemo") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://postgres:5432/warehouse"
    props = {
        "user": "warehouse_user",
        "password": "warehouse_pass",
        "driver": "org.postgresql.Driver"
    }

    # Load raw table
    df = spark.read.jdbc(jdbc_url, "raw.bank_transactions", properties=props)

    # Simple safe transform
    df2 = df.withColumn("amount_num", col("amount").cast("double")) \
            .withColumn("amount_abs", spark_abs(col("amount").cast("double")))

    # Write to demo staging table
    df2.write \
       .jdbc(jdbc_url, "stg.bank_txn_spark_demo", mode="overwrite", properties=props)

    spark.stop()



if __name__ == "__main__":
    run_spark_bank_demo()