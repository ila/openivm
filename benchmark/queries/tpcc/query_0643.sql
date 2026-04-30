-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS revenue FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID HAVING SUM(OL_AMOUNT) > 1000 AND COUNT(*) > 3;
