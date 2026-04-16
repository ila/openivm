-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, REGEXP_REPLACE(C_LAST, '[^a-zA-Z]', '', 'g') AS alpha_only FROM CUSTOMER;
