-- {"operators": "OUTER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COUNT(*) FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID WHERE w.W_ID = 3 GROUP BY w.W_ID;
