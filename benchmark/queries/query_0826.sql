-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_D_ID, STDDEV_POP(O_OL_CNT) AS std_pop FROM OORDER GROUP BY O_D_ID;
