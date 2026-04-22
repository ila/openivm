-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:ORDER", "ducklake": true}
SELECT C_W_ID, C_ID, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_W_ID ORDER BY C_ID ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS smooth_bal FROM dl.CUSTOMER;
