-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT C_W_ID, C_ID, FORMAT('Customer {} in warehouse {}', C_ID, C_W_ID) AS label FROM CUSTOMER;
