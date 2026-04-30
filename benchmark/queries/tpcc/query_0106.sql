-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT CASE WHEN C_MIDDLE IS NULL THEN 'unknown' ELSE 'known' END FROM CUSTOMER;
