-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, DATE_TRUNC('day', OL_DELIVERY_D) AS delivery_day FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;
