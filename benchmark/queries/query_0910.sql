-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_D_ID, COUNT(*) AS cnt, AVG(H_AMOUNT) AS avg_val FROM HISTORY GROUP BY H_D_ID HAVING COUNT(*) > 0 AND AVG(H_AMOUNT) > 0;
