-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT c.C_STATE, c.C_CREDIT, COUNT(*) AS n FROM CUSTOMER c JOIN WAREHOUSE w ON c.C_W_ID = w.W_ID GROUP BY c.C_STATE, c.C_CREDIT HAVING COUNT(*) > 5 AND SUM(c.C_BALANCE) > 0;
