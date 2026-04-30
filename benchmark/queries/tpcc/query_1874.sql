-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "non_incr_reason": "fn:HISTOGRAM"}
SELECT OL_W_ID, HISTOGRAM(OL_I_ID) AS item_histogram FROM ORDER_LINE GROUP BY OL_W_ID;
