-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, TRIM(W_STREET_1) AS addr, LENGTH(TRIM(W_STREET_1)) AS addr_len FROM WAREHOUSE;
