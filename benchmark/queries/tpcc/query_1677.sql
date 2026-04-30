-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, MIN(o.O_OL_CNT) AS mn, MAX(o.O_OL_CNT) AS mx, COUNT(DISTINCT o.O_OL_CNT) AS uniq FROM DISTRICT d LEFT JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID GROUP BY d.D_W_ID;
