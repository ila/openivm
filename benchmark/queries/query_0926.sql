-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT MIN(H_AMOUNT) AS min_val, MAX(H_AMOUNT) AS max_val FROM HISTORY;
