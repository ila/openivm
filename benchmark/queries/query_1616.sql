-- {"operators": "AGGREGATE,FILTER,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT 'GC' AS credit, C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID UNION ALL SELECT 'BC', C_W_ID, AVG(C_BALANCE) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID;
