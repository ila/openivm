-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, S_QUANTITY, S_QUANTITY % 10 AS mod_10, S_QUANTITY / 2 AS halved, S_QUANTITY * S_QUANTITY AS squared_qty FROM STOCK;
