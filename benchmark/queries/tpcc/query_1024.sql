-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, I_PRICE FROM ITEM WHERE I_PRICE >= 50 AND I_PRICE IS NOT NULL;
