-- {"operators": "AGGREGATE,FILTER,UNION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE IN (SELECT MAX(I_PRICE) FROM ITEM UNION SELECT MIN(I_PRICE) FROM ITEM));
