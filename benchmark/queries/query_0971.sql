-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER"}
SELECT NO_D_ID, COUNT(*) AS cnt, SUM(NO_O_ID) AS total FROM NEW_ORDER WHERE NO_O_ID > 0 GROUP BY NO_D_ID;
