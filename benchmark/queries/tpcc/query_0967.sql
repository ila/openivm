-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT NO_D_ID, STDDEV_POP(NO_O_ID) AS std_pop FROM NEW_ORDER GROUP BY NO_D_ID;
