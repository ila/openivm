-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(*) AS cnt, AVG(S_QUANTITY) AS avg_val FROM STOCK GROUP BY S_W_ID HAVING COUNT(*) > 0 AND AVG(S_QUANTITY) > 0;
