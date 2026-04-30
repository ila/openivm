-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT ROUND(I_PRICE * 1.23, 2), SUBSTR(I_NAME, 1, 3) FROM ITEM;
