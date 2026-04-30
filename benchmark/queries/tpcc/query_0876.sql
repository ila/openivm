-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_STATE, VAR_POP(W_YTD) AS var_pop FROM WAREHOUSE GROUP BY W_STATE;
