-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, D_ID, TRIM(D_NAME) AS clean_name FROM DISTRICT;
