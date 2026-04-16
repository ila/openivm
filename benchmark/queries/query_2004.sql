-- {"operators": "AGGREGATE,ORDER,WINDOW,SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:ORDER"}
SELECT C_W_ID, AVG(rn) AS avg_row_num FROM (SELECT C_W_ID, C_ID, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS rn FROM CUSTOMER) sub GROUP BY C_W_ID;
