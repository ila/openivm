-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT h.H_W_ID, h.H_C_ID FROM HISTORY h WHERE h.H_AMOUNT = (SELECT MAX(h2.H_AMOUNT) FROM HISTORY h2 WHERE h2.H_W_ID = h.H_W_ID);
