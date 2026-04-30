-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "HISTORY"}
SELECT H_C_ID, CASE WHEN H_AMOUNT > 500 THEN 'large' WHEN H_AMOUNT > 100 THEN 'medium' ELSE 'small' END FROM HISTORY;
