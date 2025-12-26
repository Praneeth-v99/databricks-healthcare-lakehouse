SELECT
  ROUND(
    100.0 * SUM(CASE WHEN fraud_flag = 'HIGH_AMOUNT' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS fraud_rate_percentage
FROM fraud_claims_gold;
