-- {"operators": "AGGREGATE,FILTER,ORDER,WINDOW,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_STATE, cust, rnk FROM (SELECT C_STATE, COUNT(*) AS cust, RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk FROM CUSTOMER GROUP BY C_STATE) t WHERE rnk <= 5;
