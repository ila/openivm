-- {"operators": "ORDER,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT DISTINCT C_W_ID, C_D_ID, C_ID FROM CUSTOMER ORDER BY C_W_ID;
