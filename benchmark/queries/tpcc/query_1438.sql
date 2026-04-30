-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev, ROW_NUMBER() OVER (PARTITION BY OL_W_ID ORDER BY SUM(OL_AMOUNT) DESC) AS top_rank FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID;
