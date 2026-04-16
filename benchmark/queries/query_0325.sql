-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT SUBSTRING(I_NAME, 1, 5) as name_prefix, LENGTH(I_DATA) as data_len FROM ITEM;
