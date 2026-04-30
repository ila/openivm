-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE >= 0 AND C_BALANCE IS NOT NULL;
