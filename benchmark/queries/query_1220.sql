-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "ITEM"}
SELECT I_ID, CAST(I_PRICE AS VARCHAR) AS price_str, CAST(I_IM_ID AS DECIMAL) AS im_dec FROM ITEM;
