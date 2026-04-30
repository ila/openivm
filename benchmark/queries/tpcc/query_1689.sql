-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot, AVG(o.O_OL_CNT) AS avg_val FROM CUSTOMER c RIGHT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_ID;
