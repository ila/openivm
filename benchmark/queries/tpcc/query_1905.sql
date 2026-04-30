-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE d.D_YTD > (SELECT AVG(d2.D_YTD) FROM DISTRICT d2 WHERE d2.D_W_ID = d.D_W_ID);
