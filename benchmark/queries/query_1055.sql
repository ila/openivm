-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, COUNT(*) AS cnt FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY w.W_ID;
