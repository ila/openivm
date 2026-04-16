-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, NULLIF(S_QUANTITY, 0) AS safe_qty, NULLIF(S_ORDER_CNT, 0) AS safe_orders FROM STOCK;
