-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, UPPER(I_NAME) AS upper_name, LOWER(I_DATA) AS lower_data FROM ITEM;
