-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT STDDEV(D_YTD) AS std, VARIANCE(D_YTD) AS var FROM DISTRICT;
