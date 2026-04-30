-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 0;
