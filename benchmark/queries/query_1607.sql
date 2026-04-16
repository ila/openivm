-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "HISTORY"}
SELECT H_W_ID, H_C_ID, H_AMOUNT, CASE WHEN H_AMOUNT IS NULL THEN 0 ELSE H_AMOUNT END AS safe_amt FROM HISTORY;
