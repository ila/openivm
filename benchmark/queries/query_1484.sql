-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_LAST, LENGTH(C_LAST) AS name_len FROM CUSTOMER WHERE LENGTH(C_LAST) > 5;
