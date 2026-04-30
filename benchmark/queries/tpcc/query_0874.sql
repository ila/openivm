-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_STATE, VARIANCE(W_YTD) AS var_val FROM WAREHOUSE GROUP BY W_STATE;
