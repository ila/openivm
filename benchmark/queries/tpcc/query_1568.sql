-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, CONCAT_WS('|', I_NAME, I_DATA) AS joined FROM ITEM;
