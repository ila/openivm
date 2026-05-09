-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "delta": true}
SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME FROM d_WAREHOUSE w JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID;
