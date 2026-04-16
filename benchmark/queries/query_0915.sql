-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_C_W_ID, MAX(H_AMOUNT) AS max_val FROM HISTORY GROUP BY H_C_W_ID;
