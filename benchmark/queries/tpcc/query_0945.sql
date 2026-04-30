-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT MIN(I_PRICE) AS min_val, MAX(I_PRICE) AS max_val FROM ITEM;
