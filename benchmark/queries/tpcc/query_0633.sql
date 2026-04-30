-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": true, "tables": "ITEM"}
SELECT I_ID, I_NAME, I_PRICE, CAST(I_PRICE AS VARCHAR) AS price_str, CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 60 THEN 'mid' ELSE 'expensive' END AS price_tier FROM ITEM;
