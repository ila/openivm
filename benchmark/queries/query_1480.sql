-- {"operators": "DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "non_incr_reason": "kw:EXCEPT"}
SELECT DISTINCT W_ID FROM WAREHOUSE EXCEPT SELECT DISTINCT C_W_ID FROM CUSTOMER;
