-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, SUM(OL_AMOUNT * OL_QUANTITY) AS weighted_sum, ROUND(SUM(OL_AMOUNT * OL_QUANTITY) / NULLIF(SUM(OL_QUANTITY), 0), 2) AS weighted_avg FROM ORDER_LINE GROUP BY OL_W_ID;
