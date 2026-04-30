-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_ID, COUNT(*) AS n, SUM(COALESCE(h.H_AMOUNT, 0)) AS tot, AVG(h.H_AMOUNT) AS avg_val FROM CUSTOMER c RIGHT JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_ID;
