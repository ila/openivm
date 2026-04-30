-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT MIN(C_BALANCE) AS min_val, MAX(C_BALANCE) AS max_val FROM CUSTOMER;
