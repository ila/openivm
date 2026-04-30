-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_PRICE, CAST(I_PRICE AS TINYINT) AS tiny, CAST(I_PRICE AS SMALLINT) AS small, CAST(I_PRICE AS BIGINT) AS big, CAST(I_PRICE AS HUGEINT) AS huge FROM ITEM;
