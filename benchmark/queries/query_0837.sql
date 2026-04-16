-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_C_ID, STDDEV(O_OL_CNT) AS std_val FROM OORDER GROUP BY O_C_ID;
