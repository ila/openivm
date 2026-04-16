-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, COUNT(OL_DELIVERY_D) AS delivered, COUNT(*) - COUNT(OL_DELIVERY_D) AS pending FROM ORDER_LINE GROUP BY OL_W_ID;
