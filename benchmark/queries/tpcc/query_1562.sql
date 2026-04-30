-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, LEFT(I_NAME, 5) AS first5, RIGHT(I_NAME, 5) AS last5 FROM ITEM;
