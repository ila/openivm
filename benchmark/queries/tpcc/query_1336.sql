-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE = (SELECT MIN(I_PRICE) FROM ITEM);
