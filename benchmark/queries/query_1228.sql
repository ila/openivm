-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, DATE_TRUNC('week', H_DATE) AS week, SUM(H_AMOUNT) AS total FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, DATE_TRUNC('week', H_DATE);
