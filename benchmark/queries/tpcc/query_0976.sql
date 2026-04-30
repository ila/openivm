-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "NEW_ORDER", "openivm_verified": true}
SELECT STDDEV(NO_O_ID) AS std, VARIANCE(NO_O_ID) AS var FROM NEW_ORDER;
