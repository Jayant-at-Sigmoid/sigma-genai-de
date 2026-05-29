-- Query to find top spenders by customer tier for dashboard integration
SELECT c.tier, email, SUM(amount) AS total_spend
FROM dim_customer c
JOIN fact_transactions t ON c.customer_id = t.customer_id
WHERE status != 'FAILED'
GROUP BY c.tier
HAVING total_spend > 5000;
