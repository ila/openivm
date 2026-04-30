-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT H_C_W_ID, VARIANCE(H_AMOUNT) AS var_val FROM HISTORY GROUP BY H_C_W_ID;
