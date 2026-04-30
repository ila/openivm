-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT o.O_ID, o.O_W_ID FROM OORDER o WHERE NOT EXISTS (SELECT 1 FROM NEW_ORDER no WHERE no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID);
