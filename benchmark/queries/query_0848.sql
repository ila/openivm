-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT STDDEV(O_OL_CNT) AS std, VARIANCE(O_OL_CNT) AS var FROM OORDER;
