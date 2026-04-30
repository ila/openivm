-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, COUNT(*) AS cnt, SUM(W_YTD) AS total, AVG(W_YTD) AS avg_val, MIN(W_YTD) AS min_val, MAX(W_YTD) AS max_val FROM WAREHOUSE GROUP BY W_STATE;
