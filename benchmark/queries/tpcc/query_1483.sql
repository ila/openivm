-- {"operators": "SCAN", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "non_incr_reason": "kw:EXCEPT"}
SELECT I_ID FROM ITEM EXCEPT SELECT S_I_ID FROM STOCK;
