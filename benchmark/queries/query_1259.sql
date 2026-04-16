-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_AMOUNT, LAG(OL_AMOUNT, 1) OVER (PARTITION BY OL_W_ID ORDER BY OL_AMOUNT) AS prev FROM ORDER_LINE;
