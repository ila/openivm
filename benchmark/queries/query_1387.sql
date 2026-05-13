-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER ORDER BY C_BALANCE DESC LIMIT 100;
