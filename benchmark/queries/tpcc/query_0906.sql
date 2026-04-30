-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_D_ID, VAR_POP(H_AMOUNT) AS var_pop FROM HISTORY GROUP BY H_D_ID;
