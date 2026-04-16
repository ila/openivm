-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "ORDER_LINE"}
SELECT OL_W_ID, SUM(CASE WHEN OL_DELIVERY_D IS NOT NULL THEN OL_AMOUNT ELSE 0 END) AS delivered_total FROM ORDER_LINE GROUP BY OL_W_ID;
