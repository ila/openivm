-- {"operators": "OUTER_JOIN,AGGREGATE,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
WITH w_agg AS (SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(wa.tot, 0) AS cust_total FROM WAREHOUSE w LEFT JOIN w_agg wa ON w.W_ID = wa.C_W_ID;
