-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, VARIANCE(O_OL_CNT) AS var_val FROM OORDER GROUP BY O_W_ID;
