-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_D_ID, AVG(H_AMOUNT) AS avg_val FROM HISTORY GROUP BY H_W_ID, H_D_ID HAVING AVG(H_AMOUNT) > 0;
