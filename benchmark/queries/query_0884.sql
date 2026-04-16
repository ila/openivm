-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT STDDEV(W_YTD) AS std, VARIANCE(W_YTD) AS var FROM WAREHOUSE;
