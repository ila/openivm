-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_ID, COALESCE(C_MIDDLE, 'N/A') AS middle, COALESCE(C_DATA, '') AS data FROM CUSTOMER;
