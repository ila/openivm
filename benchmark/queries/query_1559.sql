-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, W_NAME, LPAD(W_NAME, 20, '*') AS padded, RPAD(W_NAME, 20, '-') AS rpadded FROM WAREHOUSE;
