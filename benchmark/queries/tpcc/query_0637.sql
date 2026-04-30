-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": true, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, CASE WHEN OL_DELIVERY_D IS NULL THEN 'undelivered' ELSE 'delivered' END AS status, COALESCE(CAST(OL_DELIVERY_D AS VARCHAR), 'N/A') AS delivery_str FROM ORDER_LINE;
