-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COUNT(*) FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID WHERE d.D_YTD > 0 GROUP BY w.W_ID;
