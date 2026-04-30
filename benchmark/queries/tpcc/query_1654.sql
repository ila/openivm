-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, MIN(o.O_OL_CNT) AS mn, MAX(o.O_OL_CNT) AS mx, COUNT(DISTINCT o.O_OL_CNT) AS uniq FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY w.W_ID;
