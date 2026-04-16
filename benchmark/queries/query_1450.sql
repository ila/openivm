-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, (SELECT COUNT(*) FROM ORDER_LINE WHERE OL_I_ID = s.S_I_ID AND OL_SUPPLY_W_ID = s.S_W_ID) AS sales_count FROM STOCK s;
