-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT COALESCE(w.W_ID, d.D_W_ID) AS wid, w.W_NAME, COUNT(d.D_ID) AS num_districts FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY COALESCE(w.W_ID, d.D_W_ID), w.W_NAME;
