-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_AMOUNT, AVG(OL_AMOUNT) OVER (PARTITION BY OL_W_ID) AS part_avg FROM ORDER_LINE;
