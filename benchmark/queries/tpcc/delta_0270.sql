-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,ITEM,STOCK", "delta": true}
SELECT * FROM d_WAREHOUSE w JOIN d_STOCK s ON w.W_ID = s.S_W_ID JOIN d_ITEM i ON s.S_I_ID = i.I_ID;
