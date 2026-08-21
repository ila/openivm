-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, CAST(VAR_POP(C_BALANCE) AS DECIMAL(18, 4)) AS var_pop FROM CUSTOMER GROUP BY C_W_ID;
