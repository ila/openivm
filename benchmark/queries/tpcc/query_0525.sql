-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_I_ID, OL_AMOUNT, ROUND(OL_AMOUNT / SUM(OL_AMOUNT) OVER (PARTITION BY OL_W_ID), 4) AS pct_of_warehouse FROM ORDER_LINE;
