-- {"operators": "OUTER_JOIN,AGGREGATE,LIMIT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "op:LIMIT"}
SELECT CUSTOMER.C_ID, COUNT(*) FROM CUSTOMER LEFT JOIN OORDER ON CUSTOMER.C_ID = OORDER.O_C_ID GROUP BY CUSTOMER.C_ID LIMIT 10;
