-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, AVG(OL_AMOUNT) AS avg_val FROM ORDER_LINE GROUP BY OL_W_ID;
