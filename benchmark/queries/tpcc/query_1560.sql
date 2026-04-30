-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_ID, REPLACE(C_LAST, 'a', '@') AS replaced, REVERSE(C_LAST) AS reversed FROM CUSTOMER;
