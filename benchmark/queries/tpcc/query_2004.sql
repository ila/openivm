-- {"operators": "AGGREGATE,ORDER,WINDOW,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, AVG(rn) AS avg_row_num FROM (SELECT C_W_ID, C_ID, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS rn FROM CUSTOMER) sub GROUP BY C_W_ID;
