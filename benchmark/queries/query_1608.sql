-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, D_ID, COALESCE(D_NAME, 'UNKNOWN'), COALESCE(D_STREET_1, D_STREET_2, 'NO ADDR') AS addr FROM DISTRICT;
