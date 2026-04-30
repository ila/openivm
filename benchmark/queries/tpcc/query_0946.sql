-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT STDDEV(I_PRICE) AS std, VARIANCE(I_PRICE) AS var FROM ITEM;
