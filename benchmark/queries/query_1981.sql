-- {"operators": "INNER_JOIN,AGGREGATE,HAVING,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_STATE, COUNT(DISTINCT c.C_CREDIT) AS credit_types FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE HAVING COUNT(DISTINCT c.C_CREDIT) > 1;
