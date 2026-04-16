-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT NO_D_ID, AVG(NO_O_ID) AS avg_val FROM NEW_ORDER GROUP BY NO_D_ID;
