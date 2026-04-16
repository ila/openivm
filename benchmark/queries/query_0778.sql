-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_D_ID, MAX(OL_AMOUNT) AS max_val FROM ORDER_LINE GROUP BY OL_D_ID;
