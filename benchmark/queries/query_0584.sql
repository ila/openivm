-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE FROM ITEM i WHERE i.I_PRICE > (SELECT AVG(ol.OL_AMOUNT) FROM ORDER_LINE ol WHERE ol.OL_I_ID = i.I_ID);
