-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT d.D_W_ID, d.D_NAME, COUNT(w.W_ID) AS warehouses FROM WAREHOUSE w RIGHT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY d.D_W_ID, d.D_NAME;
