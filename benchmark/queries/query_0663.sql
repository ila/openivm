-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, COALESCE(OL_DELIVERY_D::VARCHAR, 'PENDING') AS delivery_status FROM ORDER_LINE;
