-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_AMOUNT, ROW_NUMBER() OVER (PARTITION BY OL_W_ID ORDER BY OL_AMOUNT DESC) AS rn FROM ORDER_LINE;
