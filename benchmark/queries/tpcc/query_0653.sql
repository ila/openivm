-- {"operators": "AGGREGATE,FILTER,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS total FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID, C_D_ID UNION ALL SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID, C_D_ID;
