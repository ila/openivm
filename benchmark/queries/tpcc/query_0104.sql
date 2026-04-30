-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT COUNT(*) FROM CUSTOMER WHERE C_MIDDLE IS NULL;
