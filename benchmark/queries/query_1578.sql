-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, MIN(OL_DELIVERY_D) AS first_delivery, MAX(OL_DELIVERY_D) AS last_delivery, COUNT(OL_DELIVERY_D) AS delivered FROM ORDER_LINE GROUP BY OL_W_ID;
