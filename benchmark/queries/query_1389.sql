-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, I_PRICE FROM ITEM ORDER BY I_PRICE DESC LIMIT 20;
