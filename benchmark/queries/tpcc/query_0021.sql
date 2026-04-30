-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COUNT(*) FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;
