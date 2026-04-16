-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "DISTRICT"}
SELECT D_W_ID, SUM(CASE WHEN D_NEXT_O_ID > 1000 THEN 1 ELSE 0 END) AS busy_districts FROM DISTRICT GROUP BY D_W_ID;
