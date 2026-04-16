-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, COUNT(*) FILTER (WHERE C_CREDIT = 'GC') AS good_cnt, COUNT(*) FILTER (WHERE C_CREDIT = 'BC') AS bad_cnt, SUM(C_BALANCE) FILTER (WHERE C_BALANCE > 0) AS positive_bal FROM CUSTOMER GROUP BY C_W_ID;
