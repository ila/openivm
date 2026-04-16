-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "fn:BOOL_AND"}
SELECT c.C_W_ID, BOOL_AND(o.O_ALL_LOCAL = 1) AS all_local, BOOL_OR(o.O_OL_CNT > 10) AS any_big FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID;
