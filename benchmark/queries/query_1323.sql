-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE s.S_QUANTITY < (SELECT AVG(s2.S_QUANTITY) FROM STOCK s2 WHERE s2.S_W_ID = s.S_W_ID);
