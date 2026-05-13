-- {"operators": "ORDER,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT DISTINCT C_STATE FROM CUSTOMER ORDER BY C_STATE;
