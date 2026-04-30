-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_ID, C_LAST, C_FIRST, SUBSTRING(C_LAST FROM 1 FOR 3) AS last3 FROM CUSTOMER;
