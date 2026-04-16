-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_STATE, c.C_CREDIT, COUNT(*) AS cnt, AVG(c.C_BALANCE) AS avg_bal FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE, c.C_CREDIT HAVING COUNT(*) > 10 AND AVG(c.C_BALANCE) > 0;
