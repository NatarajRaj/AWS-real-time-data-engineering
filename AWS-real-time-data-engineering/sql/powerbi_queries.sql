CREATE TABLE gold_daily_summary AS
SELECT
  DATE(event_time) AS txn_date,
  COUNT(*) AS total_transactions,
  SUM(amount) AS total_amount,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_txns
FROM silver_transactions
GROUP BY DATE(event_time);
