-- {"operators": "OUTER_JOIN,AGGREGATE,FILTER,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COUNT(d.D_ID), SUM(d.D_YTD) FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID WHERE d.D_YTD > 0 GROUP BY w.W_ID HAVING COUNT(d.D_ID) > 0;
