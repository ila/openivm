-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, CASE WHEN OL_DELIVERY_D IS NULL THEN 'pending' ELSE 'delivered' END AS status FROM ORDER_LINE;
