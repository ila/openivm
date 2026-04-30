-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_OL_CNT >= (SELECT AVG(o2.O_OL_CNT) FROM OORDER o2 WHERE o2.O_W_ID = o.O_W_ID AND o2.O_D_ID = o.O_D_ID);
