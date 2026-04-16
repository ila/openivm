-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, COUNT(*) AS cnt, AVG(D_YTD) AS avg_val FROM DISTRICT GROUP BY D_W_ID HAVING COUNT(*) > 0 AND AVG(D_YTD) > 0;
