-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT COUNT(*) AS cnt, SUM(NO_O_ID) AS total, AVG(NO_O_ID) AS avg_val FROM NEW_ORDER;
