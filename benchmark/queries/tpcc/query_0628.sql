-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, VARIANCE(C_BALANCE) AS var_bal, STDDEV(C_BALANCE) AS std_bal, STDDEV_POP(C_BALANCE) AS std_pop, VAR_POP(C_BALANCE) AS var_pop FROM CUSTOMER GROUP BY C_W_ID, C_D_ID;
