-- {"operators": "AGGREGATE,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
WITH mat AS (SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS tot, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) SELECT C_W_ID, SUM(tot) AS w_tot, SUM(n) AS w_cnt, COUNT(*) AS dists FROM mat GROUP BY C_W_ID;
