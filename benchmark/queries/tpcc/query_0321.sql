-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": false, "tables": "CUSTOMER"}
SELECT C_ID, COALESCE(C_MIDDLE, 'N/A') as middle_initial, COALESCE(CAST(C_DISCOUNT AS VARCHAR), 'none') as discount FROM CUSTOMER;
