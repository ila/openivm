-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT UPPER(C_LAST) as last_upper, LENGTH(C_FIRST) as first_len, CONCAT(C_FIRST, ' ', C_LAST) as full_name FROM CUSTOMER;
