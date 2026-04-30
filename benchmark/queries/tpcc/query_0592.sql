-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY < (SELECT AVG(S_QUANTITY) FROM STOCK);
