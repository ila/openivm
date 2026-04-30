-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT d.D_W_ID, d.D_ID, d.D_NAME, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) AS cust_count FROM DISTRICT d;
