-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_I_ID, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_I_ID;
