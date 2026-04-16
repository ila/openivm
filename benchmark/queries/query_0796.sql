-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_O_ID, COUNT(DISTINCT OL_AMOUNT) AS unique_vals FROM ORDER_LINE GROUP BY OL_O_ID;
