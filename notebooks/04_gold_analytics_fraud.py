# Databricks notebook source
silver_df = spark.table("claims_silver")
silver_df.display()


# COMMAND ----------

claims_cost_trend_df = spark.sql("""
SELECT
  claim_month,
  COUNT(*) AS total_claims,
  SUM(claim_amount) AS total_claim_amount,
  AVG(claim_amount) AS avg_claim_amount
FROM claims_silver
GROUP BY claim_month
ORDER BY claim_month
""")

claims_cost_trend_df.display()


# COMMAND ----------

(
  claims_cost_trend_df
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("claims_cost_trend_gold")
)

print("✅ claims_cost_trend_gold created")


# COMMAND ----------

fraud_df = spark.sql("""
SELECT *,
  CASE
    WHEN claim_amount > 3000 THEN 'HIGH_AMOUNT'
    ELSE 'NORMAL'
  END AS fraud_flag
FROM claims_silver
""")

fraud_df.display()


# COMMAND ----------

(
  fraud_df
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("fraud_claims_gold")
)

print("✅ fraud_claims_gold created")


# COMMAND ----------

spark.sql("SHOW TABLES").display()


# COMMAND ----------

spark.sql("""
SELECT fraud_flag, COUNT(*) AS cnt
FROM fraud_claims_gold
GROUP BY fraud_flag
""").display()
