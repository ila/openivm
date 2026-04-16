-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_O_ID, OL_AMOUNT, COUNT(*) FROM ORDER_LINE GROUP BY OL_O_ID, OL_AMOUNT;
