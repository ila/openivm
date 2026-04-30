-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE FROM ITEM i WHERE i.I_PRICE > (SELECT AVG(i2.I_PRICE) FROM ITEM i2 WHERE i2.I_IM_ID = i.I_IM_ID);
