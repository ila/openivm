-- {"operators": "AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "ivm_type": "AGGREGATE_GROUP"}
SELECT OL_W_ID, OL_D_ID, COUNT(DISTINCT OL_I_ID) AS unique_items, COUNT(*) AS total_lines, SUM(OL_AMOUNT) AS total_amount FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID;
