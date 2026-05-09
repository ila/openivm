-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER", "delta": true}
SELECT w.W_NAME AS warehouse, d.D_NAME AS district, c.C_LAST AS customer FROM d_WAREHOUSE w JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID JOIN d_CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;
