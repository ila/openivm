-- {"operators": "OUTER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, COUNT(c.C_ID) AS cnt FROM WAREHOUSE w LEFT JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID HAVING COUNT(c.C_ID) > 0;
