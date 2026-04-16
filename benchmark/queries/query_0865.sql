-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT COUNT(*) AS cnt, SUM(D_YTD) AS total, AVG(D_YTD) AS avg_val FROM DISTRICT;
