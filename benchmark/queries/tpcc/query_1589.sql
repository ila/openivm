-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, MIN(OL_AMOUNT) AS min_amt, MAX(OL_AMOUNT) AS max_amt, MAX(OL_AMOUNT) - MIN(OL_AMOUNT) AS range_amt FROM ORDER_LINE GROUP BY OL_W_ID;
