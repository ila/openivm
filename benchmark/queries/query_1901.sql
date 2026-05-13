-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_ID IN (SELECT no.NO_O_ID FROM NEW_ORDER no WHERE no.NO_W_ID = o.O_W_ID AND no.NO_D_ID = o.O_D_ID);
