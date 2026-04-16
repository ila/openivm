-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, IFNULL(C_MIDDLE, 'NA') AS mid FROM CUSTOMER;
