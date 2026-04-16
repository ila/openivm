-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, REPEAT('*', LEAST(CAST(I_PRICE AS INTEGER), 10)) AS bars FROM ITEM;
