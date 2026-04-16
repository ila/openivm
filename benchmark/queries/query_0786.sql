-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_D_ID, COUNT(*) AS cnt, AVG(OL_AMOUNT) AS avg_val FROM ORDER_LINE GROUP BY OL_D_ID HAVING COUNT(*) > 0 AND AVG(OL_AMOUNT) > 0;
