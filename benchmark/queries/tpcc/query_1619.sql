-- {"operators": "INNER_JOIN,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT DISTINCT w.W_STATE, d.D_NAME FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;
