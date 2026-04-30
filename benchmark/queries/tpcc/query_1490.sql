-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_STATE, COUNT(DISTINCT c.C_ID) AS cust, SUM(c.C_BALANCE) AS bal, AVG(c.C_DISCOUNT) AS avg_disc FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE;
