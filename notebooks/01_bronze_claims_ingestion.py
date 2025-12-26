# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

def generate_claims(n=5000):
    data = []
    for i in range(n):
        claim_date = datetime.now() - timedelta(days=random.randint(0, 90))
        data.append((
            f"C{100000+i}",
            f"M{random.randint(1,500)}",
            f"P{random.randint(1,100)}",
            random.choice(["99213","99214","93000","80050"]),
            round(random.uniform(50, 5000), 2),
            claim_date
        ))
    return data

schema = StructType([
    StructField("claim_id", StringType()),
    StructField("member_id", StringType()),
    StructField("provider_id", StringType()),
    StructField("procedure_code", StringType()),
    StructField("claim_amount", DoubleType()),
    StructField("claim_date", TimestampType())
])

claims_df = spark.createDataFrame(generate_claims(), schema)
claims_df.display()


# COMMAND ----------

from pyspark.sql import functions as F

(
  claims_df
  .withColumn("ingest_ts", F.current_timestamp())
  .write
  .format("delta")
  .mode("overwrite")   # first run only
  .saveAsTable("claims_bronze")
)

print("✅ Managed Delta table claims_bronze created")


# COMMAND ----------

spark.sql("SELECT * FROM claims_bronze LIMIT 10").display()


# COMMAND ----------

spark.sql("SELECT COUNT(*) AS total_rows FROM claims_bronze").display()

# COMMAND ----------

spark.sql("SHOW TABLES").display()