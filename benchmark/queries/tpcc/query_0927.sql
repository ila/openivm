-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY", "openivm_verified": true}
SELECT STDDEV(H_AMOUNT) AS std, VARIANCE(H_AMOUNT) AS var FROM HISTORY;
