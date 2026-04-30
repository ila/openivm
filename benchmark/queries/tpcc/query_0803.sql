-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT STDDEV(OL_AMOUNT) AS std, VARIANCE(OL_AMOUNT) AS var FROM ORDER_LINE;
