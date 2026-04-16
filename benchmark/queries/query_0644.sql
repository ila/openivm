-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(*) AS items, SUM(S_QUANTITY) AS total FROM STOCK GROUP BY S_W_ID HAVING STDDEV(S_QUANTITY) > 10;
