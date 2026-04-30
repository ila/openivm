-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT DATE_TRUNC('day', H_DATE) AS day, H_W_ID, SUM(H_AMOUNT) AS daily_total, COUNT(*) AS payments, AVG(H_AMOUNT) AS avg_payment FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY DATE_TRUNC('day', H_DATE), H_W_ID;
