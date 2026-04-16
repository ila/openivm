-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, COUNT(*) AS cnt, AVG(W_YTD) AS avg_val FROM WAREHOUSE GROUP BY W_STATE HAVING COUNT(*) > 0 AND AVG(W_YTD) > 0;
