# Databricks notebook source
bronze_df = spark.table("claims_bronze")
bronze_df.display()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dedup_window = Window.partitionBy("claim_id").orderBy(F.col("ingest_ts").desc())

silver_df = (
    bronze_df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)


# COMMAND ----------

silver_df = (
    silver_df
    .filter(F.col("claim_amount") > 0)
    .filter(F.col("procedure_code").isNotNull())
    .withColumn("procedure_code", F.upper(F.col("procedure_code")))
    .withColumn("claim_month", F.date_format("claim_date", "yyyy-MM"))
)


# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")   # first version
    .saveAsTable("claims_silver")
)

print("✅ claims_silver table created")


# COMMAND ----------

spark.sql("SELECT * FROM claims_silver LIMIT 10").display()

# COMMAND ----------

spark.sql("""
SELECT
  (SELECT COUNT(*) FROM claims_bronze) AS bronze_count,
  (SELECT COUNT(*) FROM claims_silver) AS silver_count
""").display()


# COMMAND ----------

spark.sql("SHOW TABLES").display()
