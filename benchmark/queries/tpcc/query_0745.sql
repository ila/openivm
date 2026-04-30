-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT COUNT(*) AS cnt, SUM(S_QUANTITY) AS total, AVG(S_QUANTITY) AS avg_val FROM STOCK;
