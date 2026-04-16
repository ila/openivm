-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT i.I_ID, i.I_NAME FROM ITEM i WHERE (SELECT SUM(s.S_QUANTITY) FROM STOCK s WHERE s.S_I_ID = i.I_ID) > 100;
