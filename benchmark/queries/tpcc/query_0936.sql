-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, VARIANCE(I_PRICE) AS var_val FROM ITEM GROUP BY I_IM_ID;
