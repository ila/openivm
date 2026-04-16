-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT STDDEV(S_QUANTITY) AS std, VARIANCE(S_QUANTITY) AS var FROM STOCK;
