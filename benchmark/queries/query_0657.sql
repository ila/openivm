-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, STDDEV(S_QUANTITY) AS std, VARIANCE(S_QUANTITY) AS var, STDDEV_POP(S_QUANTITY) AS std_pop, VAR_POP(S_QUANTITY) AS var_pop FROM STOCK GROUP BY S_W_ID;
