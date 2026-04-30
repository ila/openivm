-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, STARTS_WITH(C_LAST, 'B') AS starts_b, ENDS_WITH(C_FIRST, 'a') AS ends_a FROM CUSTOMER WHERE C_LAST IS NOT NULL AND C_FIRST IS NOT NULL;
