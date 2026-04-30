-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT COALESCE(NULLIF(C_MIDDLE, 'N/A'), 'unknown') FROM CUSTOMER;
