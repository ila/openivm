-- {"operators": "CROSS_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, d.D_ID FROM WAREHOUSE w CROSS JOIN DISTRICT d;
