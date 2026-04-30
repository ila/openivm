-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:MEDIAN"}
SELECT C_W_ID, SUM(C_BALANCE) AS tot_bal, AVG(C_BALANCE) AS avg_bal, MEDIAN(C_BALANCE) AS med_bal FROM CUSTOMER GROUP BY C_W_ID;
