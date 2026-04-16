-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, C_STATE, CASE WHEN AVG(C_BALANCE) > 1000 THEN 'high_avg' ELSE 'low_avg' END AS avg_tier, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_STATE;
