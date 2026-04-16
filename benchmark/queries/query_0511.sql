-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_O_ID, OL_NUMBER, OL_AMOUNT, COUNT(*) OVER (PARTITION BY OL_O_ID, OL_W_ID) AS lines_per_order FROM ORDER_LINE;
