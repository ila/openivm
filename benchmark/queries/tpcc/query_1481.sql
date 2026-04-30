-- {"operators": "DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE", "non_incr_reason": "kw:INTERSECT"}
SELECT DISTINCT OL_I_ID FROM ORDER_LINE INTERSECT SELECT DISTINCT S_I_ID FROM STOCK;
