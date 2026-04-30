-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT NO_W_ID, COUNT(*) AS cnt, SUM(NO_O_ID) AS total, AVG(NO_O_ID) AS avg_val, MIN(NO_O_ID) AS min_val, MAX(NO_O_ID) AS max_val FROM NEW_ORDER GROUP BY NO_W_ID;
