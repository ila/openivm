-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, COUNT(*) AS cnt, COUNT(DISTINCT o.O_ID) AS uniq FROM WAREHOUSE w LEFT JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY w.W_ID;
