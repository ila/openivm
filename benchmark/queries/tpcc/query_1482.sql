-- {"operators": "DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "non_incr_reason": "kw:INTERSECT"}
SELECT DISTINCT C_W_ID FROM CUSTOMER INTERSECT SELECT DISTINCT H_C_W_ID FROM HISTORY;
