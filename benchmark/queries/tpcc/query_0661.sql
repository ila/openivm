-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "non_incr_reason": "fn:APPROX_COUNT_DISTINCT"}
SELECT OL_W_ID, APPROX_COUNT_DISTINCT(OL_I_ID) AS approx_unique_items, COUNT(DISTINCT OL_I_ID) AS exact_unique_items FROM ORDER_LINE GROUP BY OL_W_ID;
