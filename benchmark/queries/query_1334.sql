-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > (SELECT AVG(C_BALANCE) FROM CUSTOMER);
