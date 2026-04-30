-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME FROM dl.WAREHOUSE w JOIN dl.DISTRICT d ON w.W_ID = d.D_W_ID;
