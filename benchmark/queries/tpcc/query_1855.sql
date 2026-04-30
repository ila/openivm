-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, CHAR_LENGTH(C_LAST) AS clen, CHARACTER_LENGTH(C_FIRST) AS flen FROM CUSTOMER WHERE C_LAST IS NOT NULL;
