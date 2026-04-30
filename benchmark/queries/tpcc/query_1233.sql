-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_ID, C_MIDDLE, COALESCE(C_MIDDLE, 'XX') AS mid FROM CUSTOMER;
