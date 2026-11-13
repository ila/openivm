-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE = (SELECT MIN(I_PRICE) FROM ITEM);
