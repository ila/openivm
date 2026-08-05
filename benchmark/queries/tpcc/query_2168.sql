-- {"operators": "TOP_K,AGGREGATE,ORDER_BY,LIMIT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_I_ID, SUM(OL_AMOUNT) AS revenue FROM ORDER_LINE GROUP BY OL_I_ID ORDER BY revenue DESC, OL_I_ID LIMIT 10;
