-- {"operators": "SEMI_JOIN,IN", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "openivm_verified": true}
SELECT I_ID, I_NAME FROM ITEM WHERE I_ID IN (SELECT S_I_ID FROM STOCK WHERE S_QUANTITY > 70);
