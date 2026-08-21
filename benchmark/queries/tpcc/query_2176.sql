-- {"operators": "PROJECTION,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_D_ID, OL_O_ID, CASE WHEN OL_DELIVERY_D IS NULL THEN 'pending' ELSE 'delivered' END AS delivery_state FROM ORDER_LINE WHERE OL_AMOUNT >= 0;
