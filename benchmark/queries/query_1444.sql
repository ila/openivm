-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot, SUM(SUM(OL_AMOUNT)) OVER (PARTITION BY OL_W_ID ORDER BY OL_O_ID ROWS UNBOUNDED PRECEDING) AS cumul FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID;
