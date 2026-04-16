-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT D_W_ID, STDDEV_POP(D_YTD) AS std_pop FROM DISTRICT GROUP BY D_W_ID;
