-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT STDDEV(C_BALANCE) AS std, VARIANCE(C_BALANCE) AS var FROM CUSTOMER;
