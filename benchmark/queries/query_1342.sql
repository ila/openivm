-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT O_W_ID, O_ID FROM OORDER WHERE O_ID IN (SELECT NO_O_ID FROM NEW_ORDER);
