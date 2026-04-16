-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, CAST(S_QUANTITY AS DECIMAL(10,2)) AS qty_dec FROM STOCK;
