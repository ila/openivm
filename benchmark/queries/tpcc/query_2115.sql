-- {"operators": "SAMPLE,AGGREGATE", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "non_incr_reason": "op:SAMPLE"}
SELECT OL_W_ID, SUM(OL_AMOUNT) AS sampled_amount FROM (SELECT * FROM ORDER_LINE USING SAMPLE reservoir(25 ROWS) REPEATABLE (7)) sampled_order_line GROUP BY OL_W_ID;
