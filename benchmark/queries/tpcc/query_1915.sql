-- {"operators": "LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "non_incr_reason": "op:LATERAL"}
SELECT w.W_ID, stats.* FROM WAREHOUSE w, LATERAL (SELECT COUNT(*) AS n_cust, SUM(C_BALANCE) AS total_bal, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_W_ID = w.W_ID) stats;
