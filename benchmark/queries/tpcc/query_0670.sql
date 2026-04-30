-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, OL_DELIVERY_D - INTERVAL '7 days' AS pickup_date FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;
