-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, STRFTIME(OL_DELIVERY_D, '%Y-%m') AS month_str FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;
