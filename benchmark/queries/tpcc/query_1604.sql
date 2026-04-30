-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, IF(C_BALANCE > 0, 'positive', 'negative') AS sign_cat FROM CUSTOMER;
