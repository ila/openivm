-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_D_ID, COUNT(*) AS cnt, SUM(H_AMOUNT) AS total, AVG(H_AMOUNT) AS avg_val, MIN(H_AMOUNT) AS min_val, MAX(H_AMOUNT) AS max_val FROM HISTORY GROUP BY H_D_ID;
