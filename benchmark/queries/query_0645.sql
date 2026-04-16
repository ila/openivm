-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal, STDDEV(C_BALANCE) AS std_bal FROM CUSTOMER GROUP BY C_W_ID HAVING STDDEV(C_BALANCE) > 100 AND AVG(C_BALANCE) > 0;
