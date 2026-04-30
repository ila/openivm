-- {"operators": "OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME FROM WAREHOUSE w RIGHT JOIN DISTRICT d ON w.W_ID = d.D_W_ID;
