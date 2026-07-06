-- {"operators": "DISTINCT,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, COUNT(DISTINCT OL_I_ID) AS distinct_items FROM ORDER_LINE GROUP BY OL_W_ID;
