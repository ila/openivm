-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_D_ID, H_AMOUNT FROM HISTORY WHERE H_AMOUNT >= 10 AND H_AMOUNT IS NOT NULL;
