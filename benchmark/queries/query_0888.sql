-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, MIN(H_AMOUNT) AS min_val FROM HISTORY GROUP BY H_W_ID;
