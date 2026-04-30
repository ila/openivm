-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, COUNT(*) AS cnt FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID;
