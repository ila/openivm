-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT NO_D_ID, MIN(NO_O_ID) AS min_val FROM NEW_ORDER GROUP BY NO_D_ID;
