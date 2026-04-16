-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, MD5(I_NAME) AS hash, HASH(I_ID) AS id_hash FROM ITEM;
