-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, COUNT(o.O_ID) AS cnt FROM WAREHOUSE w LEFT JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY w.W_ID HAVING COUNT(o.O_ID) > 0;
