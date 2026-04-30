-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, w.W_NAME, cs.avg_bal FROM WAREHOUSE w JOIN (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER GROUP BY C_W_ID) cs ON w.W_ID = cs.C_W_ID;
