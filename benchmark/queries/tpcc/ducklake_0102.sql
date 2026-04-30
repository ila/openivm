-- {"operators": "CROSS_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, d.D_ID FROM dl.WAREHOUSE w CROSS JOIN dl.DISTRICT d WHERE d.D_W_ID = w.W_ID;
