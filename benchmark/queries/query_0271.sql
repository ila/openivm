-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, COUNT(*) FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID;
