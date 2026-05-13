-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80);
