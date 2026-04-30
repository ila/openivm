-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_D_ID, COUNT(*) as cnt, COUNT(DISTINCT C_ID) as dist_cnt, SUM(C_BALANCE) as sum_bal, AVG(C_BALANCE) as avg_bal, MIN(C_BALANCE) as min_bal, MAX(C_BALANCE) as max_bal FROM CUSTOMER GROUP BY C_W_ID, C_D_ID;
