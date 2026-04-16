-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, LENGTH(I_NAME) AS name_len, LENGTH(I_DATA) AS data_len FROM ITEM;
