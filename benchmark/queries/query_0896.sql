-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, COUNT(*) AS cnt, SUM(H_AMOUNT) AS total FROM HISTORY WHERE H_AMOUNT > 10 GROUP BY H_W_ID;
