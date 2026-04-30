-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, S_QUANTITY, ABS(S_QUANTITY - 50) AS dist_from_50, ROUND(CAST(S_QUANTITY AS DECIMAL) / 100.0, 4) AS qty_pct FROM STOCK;
