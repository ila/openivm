-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_AMOUNT, GREATEST(H_AMOUNT, 0) AS non_neg, LEAST(H_AMOUNT, 1000) AS capped FROM HISTORY;
