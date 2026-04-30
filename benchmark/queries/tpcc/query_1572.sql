-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, AGE(OL_DELIVERY_D, CAST('2020-01-01' AS TIMESTAMP)) AS age_since FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;
