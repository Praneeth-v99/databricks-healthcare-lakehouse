# Databricks notebook source
recon_df = spark.sql("""
SELECT
  (SELECT COUNT(*) FROM claims_bronze) AS bronze_count,
  (SELECT COUNT(*) FROM claims_silver) AS silver_count,
  (SELECT SUM(claim_amount) FROM claims_bronze) AS bronze_amount,
  (SELECT SUM(claim_amount) FROM claims_silver) AS silver_amount
""")

recon_df.display()


# COMMAND ----------

dq_rules_df = spark.sql("""
SELECT
  COUNT(*) AS total_records,
  SUM(CASE WHEN claim_amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts,
  SUM(CASE WHEN procedure_code IS NULL THEN 1 ELSE 0 END) AS null_procedure_codes,
  COUNT(DISTINCT claim_id) AS distinct_claims
FROM claims_silver
""")

dq_rules_df.display()


# COMMAND ----------

from pyspark.sql import functions as F

dq_rules_df \
  .withColumn("dq_run_ts", F.current_timestamp()) \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("claims_dq_results")

print("✅ claims_dq_results table created")


# COMMAND ----------

spark.sql("SELECT * FROM claims_dq_results").display()


# COMMAND ----------

