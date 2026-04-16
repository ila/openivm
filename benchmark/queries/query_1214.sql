-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "ITEM"}
SELECT I_ID, I_NAME, CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 60 THEN 'mid' ELSE 'premium' END AS category FROM ITEM;
