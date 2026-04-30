-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE c.C_PAYMENT_CNT = (SELECT MAX(c2.C_PAYMENT_CNT) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);
