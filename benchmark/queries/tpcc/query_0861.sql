-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, COUNT(*) AS cnt, SUM(D_YTD) AS total, AVG(D_YTD) AS avg_val, MIN(D_YTD) AS min_val, MAX(D_YTD) AS max_val FROM DISTRICT GROUP BY D_W_ID;
