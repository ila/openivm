-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, SPLIT_PART(I_NAME, ' ', 1) AS first_word FROM ITEM;
