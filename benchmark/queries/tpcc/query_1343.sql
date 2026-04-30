-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT H_W_ID, H_C_ID FROM HISTORY WHERE H_AMOUNT > (SELECT AVG(H_AMOUNT) FROM HISTORY);
