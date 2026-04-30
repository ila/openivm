-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT NO_W_ID, VARIANCE(NO_O_ID) AS var_val FROM NEW_ORDER GROUP BY NO_W_ID;
