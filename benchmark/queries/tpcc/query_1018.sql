-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_AMOUNT FROM HISTORY WHERE H_AMOUNT BETWEEN 10 AND 100000;
