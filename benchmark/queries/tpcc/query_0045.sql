-- {"operators": "AGGREGATE,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
WITH cust_totals AS (SELECT C_W_ID, SUM(C_BALANCE) as total FROM CUSTOMER GROUP BY C_W_ID) SELECT * FROM cust_totals;
