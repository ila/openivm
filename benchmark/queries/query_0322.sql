-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_I_ID, NULLIF(S_QUANTITY, 0) as qty_or_null FROM STOCK;
