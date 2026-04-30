-- {"operators": "AGGREGATE,GROUPING_SETS", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "ivm_type": "RECOMPUTE"}
SELECT OL_W_ID, OL_D_ID, SUM(OL_AMOUNT) AS total_amount, COUNT(*) AS line_count FROM ORDER_LINE GROUP BY ROLLUP(OL_W_ID, OL_D_ID);
