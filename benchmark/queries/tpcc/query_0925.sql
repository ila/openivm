-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT COUNT(*) AS cnt, SUM(H_AMOUNT) AS total, AVG(H_AMOUNT) AS avg_val FROM HISTORY;
