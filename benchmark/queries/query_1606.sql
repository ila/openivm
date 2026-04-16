-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_PRICE, GREATEST(I_PRICE, 10.0) AS floor_10, LEAST(I_PRICE, 100.0) AS ceil_100 FROM ITEM;
