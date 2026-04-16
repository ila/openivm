-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "non_incr_reason": "fn:PERCENTILE_CONT"}
SELECT OL_W_ID, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY OL_AMOUNT) AS median_amount FROM ORDER_LINE GROUP BY OL_W_ID;
