-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_D_ID, VARIANCE(C_BALANCE) AS var_val FROM CUSTOMER GROUP BY C_D_ID;
