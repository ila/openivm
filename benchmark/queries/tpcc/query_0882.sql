-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT COUNT(*) AS cnt, SUM(W_YTD) AS total, AVG(W_YTD) AS avg_val FROM WAREHOUSE;
