-- {"operators": "AGGREGATE,FILTER,TABLE_FUNCTION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT r AS warehouse_id, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = r) AS cust_count FROM generate_series(1, 10) t(r);
