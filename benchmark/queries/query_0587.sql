-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT d.D_W_ID, d.D_ID, d.D_NAME FROM DISTRICT d WHERE (SELECT SUM(C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) > 10000;
