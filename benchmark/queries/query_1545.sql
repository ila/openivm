-- {"operators": "AGGREGATE,ORDER,WINDOW,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
WITH w_totals AS (SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID) SELECT C_W_ID, tot, RANK() OVER (ORDER BY tot DESC) AS rnk, tot / SUM(tot) OVER () AS pct FROM w_totals;
