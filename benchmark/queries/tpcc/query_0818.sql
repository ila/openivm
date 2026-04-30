-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, COUNT(*) AS cnt, AVG(O_OL_CNT) AS avg_val FROM OORDER GROUP BY O_W_ID HAVING COUNT(*) > 0 AND AVG(O_OL_CNT) > 0;
