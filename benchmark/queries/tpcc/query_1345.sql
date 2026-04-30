-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT I_ID, I_NAME FROM ITEM WHERE I_ID NOT IN (SELECT DISTINCT S_I_ID FROM STOCK WHERE S_QUANTITY = 0);
